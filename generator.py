import os
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import requests
from common import retry, log

rng = random.Random(42)
url = os.getenv("PRODUCER_URL", "http://producer:8000")

def sequence(user, day):
 session = str(uuid4())
 movie = "movie-" + str(rng.randint(1, 15))
 device = rng.choice(["MOBILE", "DESKTOP", "TV", "TABLET"])
 duration = rng.randint(1800, 7200)
 steps = [("SEARCHED", 0), ("VIEW_STARTED", 0), ("VIEW_PAUSED", duration // 3), ("VIEW_RESUMED", duration // 3)]
 if rng.random() < .8:
  steps.append(("VIEW_FINISHED", duration))
  if rng.random() < .6:
   steps.append(("LIKED", duration))
 start = day.replace(hour=10, minute=0, second=0, microsecond=0)
 for i, (kind, progress) in enumerate(steps):
  yield {"event_id": str(uuid4()), "user_id": user, "movie_id": movie, "event_type": kind, "timestamp": (start + timedelta(seconds=progress + i)).isoformat(), "device_type": device, "session_id": session, "progress_seconds": progress}

def send(e):
 r = requests.post(url + "/events", json=e, timeout=150)
 r.raise_for_status()

if __name__ == "__main__":
 today = datetime.now(timezone.utc)
 for n in range(40):
  first = today - timedelta(days=rng.randint(8, 14))
  for age in range((today.date() - first.date()).days + 1):
   if age == 0 or rng.random() < .55:
    for e in sequence("demo-" + str(n), first + timedelta(days=age)):
     retry(lambda: send(e))
 open('/tmp/ready', 'w').write('ok')
 log.info("historical_seed_finished")
 while True:
  for e in sequence("live-" + str(rng.randint(1, 100)), datetime.now(timezone.utc) - timedelta(days=1)):
   retry(lambda: send(e))
  time.sleep(float(os.getenv("GENERATOR_INTERVAL", "5")))
