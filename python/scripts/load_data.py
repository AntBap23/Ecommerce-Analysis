"""
load_data.py
------------
Load project CSVs from `data/` into PostgreSQL.

All connection and load settings come from the environment (e.g. `.env` in the
project root). Required variables:

    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    LOAD_MODE   (replace | append | fail; append creates the table if it is missing)
    PG_SCHEMA

`PG_PASSWORD` must be set; use an empty value if you have no password (`PG_PASSWORD=`).

Optional CLI overrides: `--mode`, `--schema`.

Usage:
    python load_data.py
    python load_data.py path/to/extra.csv

Requirements:
    pip install psycopg2-binary pandas python-dotenv
"""

import io
import os
import sys
import argparse
from pathlib import Path
from typing import List

import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Project root: .../Ecommerce Analysis/  (parent of python/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = _PROJECT_ROOT / "data"

load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv()


def _env_nonempty(key: str) -> str:
    v = os.getenv(key)
    if v is None or not str(v).strip():
        print(f"Missing or empty environment variable: {key}")
        sys.exit(1)
    return str(v).strip()


def _env_password() -> str:
    if "PG_PASSWORD" not in os.environ:
        print("Missing environment variable: PG_PASSWORD (use PG_PASSWORD= in .env if none)")
        sys.exit(1)
    return os.environ["PG_PASSWORD"]


def db_config_from_env():
    """PostgreSQL connection dict from env only (no defaults in code)."""
    try:
        port = int(_env_nonempty("PG_PORT"))
    except ValueError:
        print("PG_PORT must be an integer")
        sys.exit(1)
    return {
        "host": _env_nonempty("PG_HOST"),
        "port": port,
        "dbname": _env_nonempty("PG_DB"),
        "user": _env_nonempty("PG_USER"),
        "password": _env_password(),
    }


def get_connection(db_config):
    return psycopg2.connect(**db_config)


def table_name_from_file(filepath: Path) -> str:
    base = filepath.stem
    return base.lower().replace(" ", "_").replace("-", "_")


def infer_pg_type(dtype) -> str:
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "BIGINT"
    if "float" in dtype_str:
        return "DOUBLE PRECISION"
    if "bool" in dtype_str:
        return "BOOLEAN"
    if "datetime" in dtype_str:
        return "TIMESTAMP"
    return "TEXT"


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def create_table(conn, table: str, schema: str, df: pd.DataFrame, mode: str):
    qualified = f"{schema}.{table}"
    col_defs = ",\n    ".join(
        f'"{col}" {infer_pg_type(df[col].dtype)}' for col in df.columns
    )
    with conn.cursor() as cur:
        if mode == "replace":
            cur.execute(f"DROP TABLE IF EXISTS {qualified}")
            cur.execute(f"CREATE TABLE {qualified} (\n    {col_defs}\n)")
        elif mode == "fail":
            cur.execute(f"CREATE TABLE {qualified} (\n    {col_defs}\n)")
    conn.commit()


def load_csv(filepath: Path, conn, mode: str, schema: str):
    table = table_name_from_file(filepath)
    print(f"  → {filepath}  ▸  {schema}.{table}", end="  ", flush=True)

    df = pd.read_csv(filepath)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    row_count = len(df)

    if mode == "append":
        if not table_exists(conn, schema, table):
            create_table(conn, table, schema, df, "fail")
    else:
        create_table(conn, table, schema, df, mode)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    qualified = f"{schema}.{table}"
    cols = ", ".join(f'"{c}"' for c in df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {qualified} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",
            buffer,
        )
    conn.commit()

    print(f"{row_count:,} rows ✓")
    return row_count


def default_csv_paths() -> List[Path]:
    """All CSV files in the project `data/` folder (sorted by name)."""
    if not DATA_DIR.is_dir():
        print(f"Data folder not found: {DATA_DIR}")
        sys.exit(1)
    paths = sorted(DATA_DIR.glob("*.csv"), key=lambda p: p.name.lower())
    if not paths:
        print(f"No *.csv files in {DATA_DIR}")
        sys.exit(1)
    return paths


def main():
    parser = argparse.ArgumentParser(description="Load project CSVs into PostgreSQL.")
    parser.add_argument(
        "extra",
        nargs="*",
        type=Path,
        help="Optional extra CSV paths (in addition to all *.csv in <project>/data/)",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append", "fail"],
        default=None,
        help="Table creation mode (default: LOAD_MODE from environment)",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Target PostgreSQL schema (default: PG_SCHEMA from environment)",
    )
    args = parser.parse_args()

    db_config = db_config_from_env()
    mode = args.mode if args.mode is not None else _env_nonempty("LOAD_MODE")
    if mode not in ("replace", "append", "fail"):
        print("LOAD_MODE must be one of: replace, append, fail")
        sys.exit(1)
    schema = args.schema if args.schema is not None else _env_nonempty("PG_SCHEMA")

    files = default_csv_paths()
    for p in args.extra:
        files.append(p.resolve())

    for p in files:
        if not p.is_file():
            print(f"Not a file: {p}")
            sys.exit(1)

    print(f"\nData directory: {DATA_DIR}")
    print(f"Connecting to {db_config['host']}:{db_config['port']}/{db_config['dbname']} …")
    try:
        conn = get_connection(db_config)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    print(f"Mode: {mode}  |  Schema: {schema}\n")

    total_rows = 0
    errors = []

    for f in files:
        try:
            total_rows += load_csv(f, conn, mode=mode, schema=schema)
        except Exception as e:
            conn.rollback()
            errors.append((f, str(e)))
            print(f"FAILED — {e}")

    conn.close()

    print(f"\n{'─'*50}")
    print(f"Done. {len(files) - len(errors)}/{len(files)} files loaded  |  {total_rows:,} total rows")
    if errors:
        print("\nErrors:")
        for path, msg in errors:
            print(f"  {path}: {msg}")
        sys.exit(1)


if __name__ == "__main__":
    main()
