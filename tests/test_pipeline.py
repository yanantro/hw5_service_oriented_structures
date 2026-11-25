import json
import random
import time
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4
import boto3
import psycopg
import pytest
import requests
from common import rows, PG
from generator import sequence
from producer import Event


def wait(fn):
 end = time.monotonic() + 150
 while time.monotonic() < end:
  result = fn()
  if result:
   return result
  time.sleep(1)
 raise AssertionError("Pipeline timeout")


def test_validation_and_generator():
 for i in range(100):
  events = list(sequence(str(i), datetime.now(timezone.utc)))
  types = [e["event_type"] for e in events]
  assert types.index("VIEW_STARTED") < types.index("VIEW_PAUSED") < types.index("VIEW_RESUMED")
  if "VIEW_FINISHED" in types:
   assert types.index("VIEW_STARTED") < types.index("VIEW_FINISHED")
  assert [e["progress_seconds"] for e in events] == sorted(e["progress_seconds"] for e in events)
  for event in events:
   Event(**event)
 with pytest.raises(ValueError):
  Event(**{**events[0], "progress_seconds": -1})
 with pytest.raises(ValueError):
  Event(**{**events[0], "timestamp": "2026-01-01T00:00:00"})


def test_pipeline_metrics_export():
 rng = random.Random(str(uuid4()))
 for attempt in range(100):
  day = date(1980, 1, 1) + timedelta(days=rng.randint(0, 5000))
  if int(rows(f"SELECT count() AS n FROM cinema.events FINAL WHERE toDate(timestamp) BETWEEN toDate('{day}') AND addDays(toDate('{day}'),7)")[0]["n"]) == 0:
   break
 else:
  raise AssertionError("Cannot allocate empty test dates")
 prefix = "test-" + str(uuid4())
 events = []
 def add(user, age, kind, progress):
  e = {"event_id": str(uuid4()), "user_id": prefix + user, "movie_id": prefix, "event_type": kind, "timestamp": f"{day + timedelta(days=age)}T12:00:00.123Z", "device_type": "TV", "session_id": prefix + user + str(age), "progress_seconds": progress, "app_version": "2.0"}
  r = requests.post("http://producer:8000/events", json=e, timeout=150)
  assert r.status_code == 201, r.text
  assert r.json()["event_id"] == e["event_id"]
  events.append(e)
 add("a", 0, "VIEW_STARTED", 0)
 add("a", 0, "VIEW_FINISHED", 1200)
 add("b", 0, "VIEW_STARTED", 0)
 add("a", 1, "SEARCHED", 0)
 add("a", 7, "VIEW_STARTED", 0)
 first = events[0]
 assert requests.post("http://producer:8000/events", json=first, timeout=150).status_code == 201
 actual = wait(lambda: rows(f"SELECT * FROM cinema.events FINAL WHERE event_id = '{first['event_id']}'"))[0]
 for key in ("event_id", "user_id", "movie_id", "event_type", "device_type", "session_id", "progress_seconds", "app_version"):
  assert actual[key] == first[key]
 assert actual["timestamp"] == first["timestamp"].replace("T", " ").replace("Z", "")
 wait(lambda: int(rows(f"SELECT count() AS n FROM cinema.events FINAL WHERE startsWith(user_id, '{prefix}')")[0]["n"]) == 5)
 assert requests.post("http://producer:8000/events", json={**first, "event_type": "INVALID"}, timeout=10).status_code == 422
 for repeat in range(2):
  r = requests.post(f"http://aggregation:8001/aggregate/{day}", timeout=180)
  assert r.status_code == 200, r.text
 with psycopg.connect(PG) as conn:
  data = conn.execute("SELECT metric, dimension, value FROM metrics WHERE date = %s", (day,)).fetchall()
 metrics = {(m, dim): v for m, dim, v in data}
 assert len(data) == len(metrics)
 assert metrics["dau", ""] == 2
 assert metrics["avg_watch_seconds", ""] == 1200
 assert metrics["conversion", ""] == .5
 assert metrics["movie_views", prefix] == 2
 assert metrics["retention_d1", ""] == .5
 assert metrics["retention_d7", ""] == .5
 assert metrics["retention", "0"] == 1
 actual = rows(f"SELECT metric, dimension, value FROM cinema.metrics WHERE date = toDate('{day}')")
 assert {(r["metric"], r["dimension"]): r["value"] for r in actual} == metrics
 client = boto3.client("s3", endpoint_url="http://minio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
 for repeat in range(2):
  r = requests.post(f"http://aggregation:8001/export/{day}", timeout=120)
  assert r.status_code == 200, r.text
 key = f"daily/{day}/aggregates.json"
 exported = json.loads(client.get_object(Bucket="movie-analytics", Key=key)["Body"].read())
 assert {(r["metric"], r["dimension"]): r["value"] for r in exported} == metrics
 assert all(r["date"] == str(day) and r["calculated_at"] for r in exported)
 assert client.list_objects_v2(Bucket="movie-analytics", Prefix=f"daily/{day}/")["KeyCount"] == 1


def test_infrastructure():
 from confluent_kafka.admin import AdminClient, ConfigResource
 admin = AdminClient({"bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092"})
 topic = admin.list_topics(timeout=15).topics["movie-events"]
 assert len(topic.partitions) >= 3
 assert all(len(p.replicas) >= 2 for p in topic.partitions.values())
 resource = ConfigResource(ConfigResource.Type.TOPIC, "movie-events")
 config = admin.describe_configs([resource])[resource].result()
 assert int(config["min.insync.replicas"].value) >= 1
 versions = requests.get("http://schema-registry:8081/subjects/movie-events-value/versions", timeout=10).json()
 assert versions == [1, 2]
 result = requests.get("http://grafana:3000/api/dashboards/uid/cinema", auth=("admin", "admin"), timeout=15)
 assert result.status_code == 200
 assert len(result.json()["dashboard"]["panels"]) >= 3
