"""Use a disposable database: parser advisory locks must not affect tests."""
import os
import subprocess
import sys
import uuid

import psycopg
from psycopg import sql


dsn = os.getenv("TEST_DATABASE_URL", "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog")
name = "catalog_test_" + uuid.uuid4().hex
with psycopg.connect(dsn, autocommit=True) as db:
    db.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
try:
    result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v", *sys.argv[1:]],
        env={**os.environ, "TEST_DATABASE_URL": psycopg.conninfo.make_conninfo(dsn, dbname=name)})
finally:
    with psycopg.connect(dsn, autocommit=True) as db:
        db.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
raise SystemExit(result.returncode)
