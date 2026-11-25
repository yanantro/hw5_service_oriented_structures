import os
import time
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cinema")
CH = os.getenv("CH_URL", "http://clickhouse:8123")
PG = os.getenv("PG_DSN", "postgresql://cinema:cinema@postgres:5432/cinema")

def retry(fn, attempts=5):
 for n in range(attempts):
  try:
   return fn()
  except Exception:
   log.exception("attempt_failed attempt=%s", n + 1)
   if n == attempts - 1:
    raise
   time.sleep(min(2 ** n, 16))

def ch(sql, params=None):
 r = requests.post(CH, params=params, data=sql.encode(), auth=("cinema", "cinema"), timeout=120)
 r.raise_for_status()
 return r.text

def rows(sql):
 return requests_json(sql)

def requests_json(sql):
 import json
 return json.loads(ch(sql + " FORMAT JSON"))["data"]
