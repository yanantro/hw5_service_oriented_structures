import json
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from contextlib import asynccontextmanager
import boto3
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from common import ch, rows, PG, retry, log

mutex = threading.Lock()
stop = threading.Event()

def calculate(day):
 d = day.isoformat()
 summary = rows(f"SELECT count() AS processed, uniqExact(user_id) AS dau, avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds, countIf(event_type = 'VIEW_STARTED') AS started, countIf(event_type = 'VIEW_FINISHED') AS finished FROM cinema.events FINAL WHERE toDate(timestamp) = toDate('{d}')")[0]
 result = [("dau", "", float(summary["dau"])), ("avg_watch_seconds", "", summary["avg_watch_seconds"]), ("conversion", "", float(summary["finished"]) / float(summary["started"]) if int(summary["started"]) else None)]
 for r in rows(f"SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views FROM cinema.events FINAL WHERE toDate(timestamp) = toDate('{d}') GROUP BY movie_id HAVING views > 0 ORDER BY views DESC, movie_id"):
  result.append(("movie_views", r["movie_id"], float(r["views"])))
 cohort = rows(f"WITH firsts AS (SELECT user_id, min(toDate(timestamp)) AS cohort FROM cinema.events FINAL WHERE event_type = 'VIEW_STARTED' GROUP BY user_id), activity AS (SELECT user_id, groupUniqArray(toDate(timestamp)) AS days FROM cinema.events FINAL GROUP BY user_id) SELECT uniqExact(f.user_id) AS size, " + ", ".join(f"uniqExactIf(f.user_id, has(a.days, addDays(f.cohort, {i}))) AS d{i}" for i in range(8)) + f" FROM firsts f INNER JOIN activity a ON f.user_id = a.user_id WHERE f.cohort = toDate('{d}')")[0]
 size = int(cohort["size"])
 result.append(("cohort_size", "", float(size)))
 for i in range(8):
  value = float(cohort[f"d{i}"]) / size if size and day + timedelta(days=i) <= datetime.now(timezone.utc).date() else None
  result.append(("retention", str(i), value))
 result.extend((f"retention_d{i}", "", next(v for m, dim, v in result if m == "retention" and dim == str(i))) for i in (1, 7))
 return result, int(summary["processed"])

def export(day):
 with psycopg.connect(PG, row_factory=dict_row) as conn:
  data = conn.execute("SELECT date, metric, dimension, value, calculated_at FROM metrics WHERE date = %s ORDER BY metric, dimension", (day,)).fetchall()
 if not data:
  raise ValueError("No aggregates for this date")
 client = boto3.client("s3", endpoint_url=os.getenv("S3_URL", "http://minio:9000"), aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
 bucket = "movie-analytics"
 try:
  client.head_bucket(Bucket=bucket)
 except client.exceptions.ClientError as e:
  if str(e.response["Error"]["Code"]) not in ("404", "NoSuchBucket"):
   raise
  client.create_bucket(Bucket=bucket)
 key = f"daily/{day.isoformat()}/aggregates.json"
 client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, default=str, ensure_ascii=False).encode(), ContentType="application/json")
 with psycopg.connect(PG) as conn:
  conn.execute("INSERT INTO export_jobs(date, exported_at) VALUES (%s, now()) ON CONFLICT(date) DO UPDATE SET exported_at = EXCLUDED.exported_at", (day,))
 log.info("export_finished date=%s rows=%s key=%s", day, len(data), key)
 return {"key": key, "rows": len(data)}

def aggregate(day):
 with mutex:
  start = time.monotonic()
  log.info("aggregation_started date=%s", day)
  try:
   values, processed = calculate(day)
   now = datetime.now(timezone.utc)
   payload = "\n".join(json.dumps({"date": day.isoformat(), "metric": m, "dimension": dim, "value": v, "calculated_at": now.strftime("%Y-%m-%d %H:%M:%S.%f")}) for m, dim, v in values)
   def stage():
    ch("TRUNCATE TABLE cinema.metrics_stage")
    ch("INSERT INTO cinema.metrics_stage FORMAT JSONEachRow\n" + payload)
   retry(stage)
   ch(f"ALTER TABLE cinema.metrics REPLACE PARTITION '{day.isoformat()}' FROM cinema.metrics_stage")
   def persist():
    with psycopg.connect(PG) as conn:
     conn.execute("DELETE FROM metrics WHERE date = %s", (day,))
     with conn.cursor() as cur:
      cur.executemany("INSERT INTO metrics(date, metric, dimension, value, calculated_at) VALUES (%s,%s,%s,%s,%s) ON CONFLICT(date, metric, dimension) DO UPDATE SET value = EXCLUDED.value, calculated_at = EXCLUDED.calculated_at", [(day, m, dim, v, now) for m, dim, v in values])
     conn.execute("INSERT INTO export_jobs(date) VALUES (%s) ON CONFLICT(date) DO UPDATE SET exported_at = NULL", (day,))
   retry(persist)
   log.info("aggregation_finished date=%s processed=%s metrics=%s seconds=%.3f", day, processed, len(values), time.monotonic() - start)
   return {"date": day, "processed": processed, "metrics": len(values)}
  except Exception:
   log.exception("aggregation_failed date=%s seconds=%.3f", day, time.monotonic() - start)
   raise

def cycle():
 today = datetime.now(timezone.utc).date()
 dates = {date.fromisoformat(r["day"]) for r in rows("SELECT DISTINCT toDate(timestamp) AS day FROM cinema.events FINAL")}
 dates.update((today, today - timedelta(days=1)))
 for day in sorted(dates):
  try:
   aggregate(day)
  except Exception:
   log.exception("scheduled_date_failed date=%s", day)
 with psycopg.connect(PG) as conn:
  pending = conn.execute("SELECT date FROM export_jobs WHERE exported_at IS NULL AND date < %s ORDER BY date", (today,)).fetchall()
 for (day,) in pending:
  try:
   retry(lambda: export(day), 3)
  except Exception:
   log.exception("export_pending_retry_next_cycle date=%s", day)

def scheduler():
 while not stop.is_set():
  try:
   cycle()
  except Exception:
   log.exception("scheduled_cycle_failed")
  stop.wait(float(os.getenv("AGGREGATION_INTERVAL", "60")))

@asynccontextmanager
async def lifespan(app):
 thread = threading.Thread(target=scheduler, daemon=True)
 thread.start()
 yield
 stop.set()
 thread.join(timeout=3)

app = FastAPI(title="Aggregation Service", lifespan=lifespan)

@app.get("/health")
def health():
 try:
  ch("SELECT 1")
  with psycopg.connect(PG, connect_timeout=3) as conn:
   conn.execute("SELECT 1")
  return {"status": "ok"}
 except Exception as e:
  raise HTTPException(503, str(e))

@app.post("/aggregate/{day}")
def manual(day: date):
 return aggregate(day)

@app.post("/export/{day}")
def manual_export(day: date):
 try:
  with mutex:
   return retry(lambda: export(day), 3)
 except ValueError as e:
  raise HTTPException(404, str(e))
