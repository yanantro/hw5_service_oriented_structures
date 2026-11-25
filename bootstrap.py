import json
import os
import requests
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from common import retry, log

registry = os.getenv("REGISTRY_URL", "http://schema-registry:8081")

def request(method, path, **kwargs):
 r = requests.request(method, registry + path, timeout=20, **kwargs)
 r.raise_for_status()
 return r.json()

def main():
 admin = AdminClient({"bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092"})
 topics = admin.list_topics(timeout=20).topics
 if "movie-events" not in topics:
  for future in admin.create_topics([NewTopic("movie-events", 3, 3, config={"min.insync.replicas": "1"})]).values():
   future.result()
 for future in admin.alter_configs([ConfigResource(ConfigResource.Type.TOPIC, "movie-events", set_config={"min.insync.replicas": "1"})]).values():
  future.result()
 request("PUT", "/config/movie-events-value", json={"compatibility": "BACKWARD_TRANSITIVE"})
 for version in (1, 2):
  schema = open(f"schemas/movie-event-v{version}.avsc").read()
  result = request("POST", "/subjects/movie-events-value/versions", json={"schemaType": "AVRO", "schema": schema})
  log.info("schema_registered file_version=%s id=%s", version, result["id"])
 log.info("versions=%s", request("GET", "/subjects/movie-events-value/versions"))
 open('/tmp/ready', 'w').write('ok')

if __name__ == "__main__":
 retry(main, 10)
