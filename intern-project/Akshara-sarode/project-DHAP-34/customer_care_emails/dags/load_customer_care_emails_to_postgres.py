from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

PROJECT_DIR = Path("/opt/airflow/project")
MANIFEST_PATH = PROJECT_DIR / "MANIFEST.md"
SCHEMA_PATH = PROJECT_DIR / "config" / "schema_expected.yaml"
DDL_PATH = PROJECT_DIR / "config" / "create_table.sql"
CSV_PATH = PROJECT_DIR / "sample_data" / "dataset.csv"
CLEANED_CSV_PATH = PROJECT_DIR / "sample_data" / "dataset_cleaned.csv"


def read_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise AirflowException(f"Missing schema file: {SCHEMA_PATH}")

    with SCHEMA_PATH.open("r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    if not schema or "table" not in schema or "columns" not in schema:
        raise AirflowException("schema_expected.yaml must contain 'table' and 'columns'.")

    return schema


def validate_source() -> None:
    if not MANIFEST_PATH.exists():
        raise AirflowException(f"Missing manifest file: {MANIFEST_PATH}")

    manifest_text = MANIFEST_PATH.read_text(encoding="utf-8").lower()
    if "status" not in manifest_text or "done" not in manifest_text:
        raise AirflowException("MANIFEST.md must include Status: done.")

    if not CSV_PATH.exists():
        raise AirflowException(f"Missing CSV file: {CSV_PATH}")

    schema = read_schema()
    df = pd.read_csv(CSV_PATH)

    expected_cols = [col["name"] for col in schema["columns"]]
    actual_cols = list(df.columns)

    if actual_cols != expected_cols:
        raise AirflowException(
            f"CSV columns do not match schema.\nExpected: {expected_cols}\nGot: {actual_cols}"
        )

    if df.empty:
        raise AirflowException("CSV is empty.")

    print(f"Validation passed for {len(df)} rows.")


def transform_csv() -> str:
    schema = read_schema()
    df = pd.read_csv(CSV_PATH)

    expected_cols = [col["name"] for col in schema["columns"]]
    df = df[expected_cols].copy()

    for col_meta in schema["columns"]:
        name = col_meta["name"]
        dtype = str(col_meta["type"]).lower()
        nullable = bool(col_meta.get("nullable", True))

        if dtype in {"string", "text"}:
            df[name] = df[name].astype("string").str.strip()
        elif dtype in {"integer", "int", "bigint", "smallint"}:
            df[name] = pd.to_numeric(df[name], errors="raise").astype("Int64")
        elif dtype.startswith("decimal") or dtype in {"numeric", "float", "double", "real"}:
            df[name] = pd.to_numeric(df[name], errors="raise")
        elif dtype == "timestamp":
            df[name] = pd.to_datetime(df[name], errors="raise")
        else:
            df[name] = df[name]

        if not nullable and df[name].isna().any():
            raise AirflowException(f"Column '{name}' has nulls but is marked nullable=false.")

    df.to_csv(CLEANED_CSV_PATH, index=False, date_format="%Y-%m-%d %H:%M:%S")
    return str(CLEANED_CSV_PATH)


def prepare_table() -> None:
    hook = PostgresHook(postgres_conn_id="target_postgres")
    ddl_sql = DDL_PATH.read_text(encoding="utf-8")
    hook.run(ddl_sql)
    print("Target table is ready.")


def load_to_postgres(cleaned_csv_path: str) -> None:
    schema = read_schema()
    table = schema["table"]

    df = pd.read_csv(cleaned_csv_path)
    expected_cols = [col["name"] for col in schema["columns"]]
    df = df[expected_cols].copy()

    for col_meta in schema["columns"]:
        name = col_meta["name"]
        dtype = str(col_meta["type"]).lower()

        if dtype == "timestamp":
            df[name] = pd.to_datetime(df[name], errors="raise")
        elif dtype in {"integer", "int", "bigint", "smallint"}:
            df[name] = pd.to_numeric(df[name], errors="raise").astype("Int64")
        elif dtype.startswith("decimal") or dtype in {"numeric", "float", "double", "real"}:
            df[name] = pd.to_numeric(df[name], errors="raise")

    columns = list(df.columns)
    records = []

    for row in df.itertuples(index=False, name=None):
        row_values = []
        for col_name, value in zip(columns, row):
            if pd.isna(value):
                row_values.append(None)
            elif col_name == "timestamp":
                row_values.append(pd.to_datetime(value).to_pydatetime())
            else:
                row_values.append(value)
        records.append(tuple(row_values))

    if not records:
        raise AirflowException("No rows to load.")

    hook = PostgresHook(postgres_conn_id="target_postgres")
    conn = hook.get_conn()
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table};")
            execute_values(cur, insert_sql, records, page_size=1000)

    print(f"Loaded {len(records)} rows into {table}.")


with DAG(
    dag_id="load_customer_care_emails_to_postgres",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["dhap-34", "customer_care_emails"],
    description="Load local CSV into PostgreSQL",
) as dag:
    validate_task = PythonOperator(
        task_id="validate_source",
        python_callable=validate_source,
    )

    transform_task = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_csv,
    )

    prepare_table_task = PythonOperator(
        task_id="prepare_table",
        python_callable=prepare_table,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_args=[transform_task.output],
    )

    validate_task >> transform_task >> prepare_table_task >> load_task
