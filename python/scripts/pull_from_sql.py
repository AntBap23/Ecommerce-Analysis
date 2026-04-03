"""
pull_from_sql.py
----------------
Notebook-friendly helpers to read analysis-ready data from Postgres into pandas.

Expects env vars (from project-root `.env`):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    PG_SCHEMA (optional; defaults to "public" if not set)

Example (in a Jupyter notebook):

    import sys
    sys.path.append("../python/scripts")   # if notebook is in project root/notebooks, adjust as needed

    from pull_from_sql import load_project_data, read_table, read_sql

    df_ab = read_table("ab_data_clean")    # schema defaults to PG_SCHEMA or "public"
    df_countries = read_table("countries")
    datasets = load_project_data()
    df_joined = datasets["joined"]
    df_custom = read_sql("select * from public.ab_data_clean limit 10;")
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

DEFAULT_AB_TABLE = "ab_data_clean"
DEFAULT_COUNTRIES_TABLE = "countries"


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


def load_project_data(
    *,
    schema: Optional[str] = None,
    ab_table: str = DEFAULT_AB_TABLE,
    countries_table: str = DEFAULT_COUNTRIES_TABLE,
    engine: Optional[Engine] = None,
) -> dict[str, pd.DataFrame]:
    """
    Load the cleaned experiment table, countries lookup, and a joined analysis table.
    """
    eng = engine or get_engine()
    sch = schema or os.getenv("PG_SCHEMA", "public")

    df_ab = read_table(ab_table, schema=sch, engine=eng)
    df_countries = read_table(countries_table, schema=sch, engine=eng)

    joined_query = f"""
        SELECT
            ab.*,
            c.country
        FROM "{sch}"."{ab_table}" AS ab
        LEFT JOIN "{sch}"."{countries_table}" AS c
            ON ab.user_id = c.user_id
    """
    df_joined = read_sql(joined_query, engine=eng)

    return {
        "ab_data_clean": df_ab,
        "countries": df_countries,
        "joined": df_joined,
    }


def main() -> None:
    """
    Small CLI for quick checks:
        python pull_from_sql.py ab_data_clean --schema public --limit 5
        python pull_from_sql.py --project-data
        python pull_from_sql.py --sql "select count(*) from public.ab_data_clean;"
    """
    import argparse

    parser = argparse.ArgumentParser(description="Pull data from Postgres into pandas.")
    parser.add_argument("table", nargs="?", help="Table name to read")
    parser.add_argument("--schema", default=None, help="Schema (defaults to PG_SCHEMA or public)")
    parser.add_argument("--limit", type=int, default=10, help="Limit for table reads (default: 10)")
    parser.add_argument("--sql", default=None, help="Raw SQL to run instead of reading a table")
    parser.add_argument(
        "--project-data",
        action="store_true",
        help=f"Load {DEFAULT_AB_TABLE} and {DEFAULT_COUNTRIES_TABLE}, plus their joined dataset",
    )
    args = parser.parse_args()

    try:
        if args.project_data:
            datasets = load_project_data(schema=args.schema)
            for name, df in datasets.items():
                print(f"\n{name} ({len(df):,} rows)")
                print(df.head(args.limit).to_string(index=False))
        elif args.sql:
            df = read_sql(args.sql)
            print(df.head(10).to_string(index=False))
        else:
            if not args.table:
                parser.error("Provide a table name, pass --sql, or use --project-data")
            df = read_table(args.table, schema=args.schema, limit=args.limit)
            print(df.head(10).to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
