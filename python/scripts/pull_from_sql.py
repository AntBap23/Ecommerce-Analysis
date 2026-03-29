"""
pull_from_sql.py
----------------
Notebook-friendly helpers to read data from Postgres into pandas.

Expects env vars (from project-root `.env`):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    PG_SCHEMA (optional; defaults to "public" if not set)

Example (in a Jupyter notebook):

    import sys
    sys.path.append("../python/scripts")   # if notebook is in project root/notebooks, adjust as needed

    from pull_from_sql import read_table, read_sql

    df_ab = read_table("ab_data")          # schema defaults to PG_SCHEMA or "public"
    df_countries = read_table("countries")
    df_custom = read_sql("select * from public.ab_data limit 10;")
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Project root: .../Ecommerce Analysis/  (parent of python/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv()


def _env_nonempty(key: str) -> str:
    v = os.getenv(key)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing or empty environment variable: {key}")
    return str(v).strip()


def _env_password() -> str:
    # Allow empty password, but require key presence for clarity.
    if "PG_PASSWORD" not in os.environ:
        raise RuntimeError("Missing environment variable: PG_PASSWORD (use PG_PASSWORD= if none)")
    return os.environ["PG_PASSWORD"]


def get_engine(*, application_name: str = "ecommerce-analysis") -> Engine:
    """
    Create a SQLAlchemy Engine from PG_* env vars.
    """
    host = _env_nonempty("PG_HOST")
    port = _env_nonempty("PG_PORT")
    db = _env_nonempty("PG_DB")
    user = _env_nonempty("PG_USER")
    password = _env_password()

    # psycopg2 driver is provided by psycopg2-binary
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, connect_args={"application_name": application_name})


def read_sql(
    query: Union[str, "text"],
    params: Optional[dict] = None,
    *,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    """
    Run a SQL query and return a DataFrame.
    """
    eng = engine or get_engine()
    sql_obj = text(query) if isinstance(query, str) else query
    with eng.connect() as conn:
        return pd.read_sql(sql_obj, conn, params=params)


def read_table(
    table: str,
    *,
    schema: Optional[str] = None,
    columns: Optional[list[str]] = None,
    limit: Optional[int] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    """
    Read a whole table (optionally subset columns / limit).
    """
    sch = schema or os.getenv("PG_SCHEMA", "public")
    cols = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    lim = f" LIMIT {int(limit)}" if limit is not None else ""
    q = f'SELECT {cols} FROM "{sch}"."{table}"{lim};'
    return read_sql(q, engine=engine)


def main() -> None:
    """
    Small CLI for quick checks:
        python pull_from_sql.py table_name --schema public --limit 5
        python pull_from_sql.py --sql "select count(*) from public.ab_data;"
    """
    import argparse

    parser = argparse.ArgumentParser(description="Pull data from Postgres into pandas.")
    parser.add_argument("table", nargs="?", help="Table name to read")
    parser.add_argument("--schema", default=None, help="Schema (defaults to PG_SCHEMA or public)")
    parser.add_argument("--limit", type=int, default=10, help="Limit for table reads (default: 10)")
    parser.add_argument("--sql", default=None, help="Raw SQL to run instead of reading a table")
    args = parser.parse_args()

    try:
        if args.sql:
            df = read_sql(args.sql)
        else:
            if not args.table:
                parser.error("Provide a table name or pass --sql")
            df = read_table(args.table, schema=args.schema, limit=args.limit)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
