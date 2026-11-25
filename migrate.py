from pathlib import Path
import psycopg
from common import ch, PG, retry

def migrate():
 for statement in Path("sql/clickhouse.sql").read_text().split(";"):
  if statement.strip():
   ch(statement)
 with psycopg.connect(PG) as conn:
  conn.execute(Path("sql/postgres.sql").read_text())
 open('/tmp/ready', 'w').write('ok')

if __name__ == "__main__":
 retry(migrate, 10)
