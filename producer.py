import io
import json
import os
import struct
import threading
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer, validation
from common import retry, log

class EventType(str, Enum):
 VIEW_STARTED = "VIEW_STARTED"
 VIEW_FINISHED = "VIEW_FINISHED"
 VIEW_PAUSED = "VIEW_PAUSED"
 VIEW_RESUMED = "VIEW_RESUMED"
 LIKED = "LIKED"
 SEARCHED = "SEARCHED"

class DeviceType(str, Enum):
 MOBILE = "MOBILE"
 DESKTOP = "DESKTOP"
 TV = "TV"
 TABLET = "TABLET"

class Event(BaseModel):
 model_config = ConfigDict(extra="forbid")
 event_id: UUID
 user_id: str = Field(min_length=1, max_length=200)
 movie_id: str = Field(min_length=1, max_length=200)
 event_type: EventType
 timestamp: datetime
 device_type: DeviceType
 session_id: str = Field(min_length=1, max_length=200)
 progress_seconds: int = Field(ge=0, le=2147483647, strict=True)
 app_version: str = "1.0"

 @field_validator("timestamp")
 @classmethod
 def utc(cls, v):
  if v.tzinfo is None or v.utcoffset() is None:
   raise ValueError("timestamp must include timezone")
  return v.astimezone(timezone.utc)

 def avro(self):
  d = self.model_dump(mode="json")
  d["timestamp"] = int(self.timestamp.timestamp() * 1000)
  return d

app = FastAPI(title="Movie Service")
producer = None
schema = None
schema_id = None
lock = threading.Lock()

def setup():
 global producer, schema, schema_id
 r = requests.get(os.getenv("REGISTRY_URL", "http://schema-registry:8081") + "/subjects/movie-events-value/versions/2", timeout=20)
 r.raise_for_status()
 data = r.json()
 schema_id = data["id"]
 schema = parse_schema(json.loads(data["schema"]))
 producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092,kafka3:9092"), "acks": "all", "enable.idempotence": True, "retries": 10, "retry.backoff.ms": 300, "delivery.timeout.ms": 30000, "max.in.flight.requests.per.connection": 5})

@app.on_event("startup")
def startup():
 retry(setup, 10)

@app.get("/health")
def health():
 try:
  producer.list_topics(timeout=3)
  return {"status": "ok"}
 except Exception as e:
  raise HTTPException(503, str(e))

@app.post("/events", status_code=201)
def publish(event: Event):
 d = event.avro()
 validation.validate(d, schema)
 b = io.BytesIO()
 b.write(struct.pack(">bI", 0, schema_id))
 schemaless_writer(b, schema, d)
 def send():
  delivered = []
  def callback(err, msg):
   delivered.append(err)
  with lock:
   producer.produce("movie-events", key=event.user_id.encode(), value=b.getvalue(), on_delivery=callback)
   remaining = producer.flush(35)
   if remaining or not delivered or delivered[0] is not None:
    raise RuntimeError(str(delivered))
 try:
  retry(send, 3)
 except Exception as e:
  raise HTTPException(503, "Kafka publication failed") from e
 log.info("published event_id=%s event_type=%s timestamp=%s", event.event_id, event.event_type.value, event.timestamp.isoformat())
 return {"event_id": str(event.event_id)}
