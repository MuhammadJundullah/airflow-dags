from datetime import datetime, timedelta

import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

default_args = {
    "owner": "sayid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ID = "project-6b44e885-577a-4e5b-82a"
DATASET_ID = "MyDataset"
PG_SCHEMA = "public"


def resolve_table_name(pg_hook: PostgresHook, name: str) -> str:
    """Cari nama tabel asli (case-insensitive) di schema Postgres."""
    row = pg_hook.get_first(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND lower(table_name) = lower(%s)",
        parameters=(PG_SCHEMA, name),
    )
    if not row:
        available = pg_hook.get_records(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            parameters=(PG_SCHEMA,),
        )
        raise ValueError(
            f"Tabel '{name}' tidak ditemukan di schema '{PG_SCHEMA}'. "
            f"Tabel yang ada: {[r[0] for r in available]}"
        )
    return row[0]


def run_etl_table(postgres_table: str, bq_table: str):
    # 1. EXTRACT
    pg_hook = PostgresHook(postgres_conn_id="1")
    real_name = resolve_table_name(pg_hook, postgres_table)
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_sql(f'SELECT * FROM {PG_SCHEMA}."{real_name}";', engine)

    # 2. TRANSFORM
    df.columns = [col.lower() for col in df.columns]

    # String kosong / hanya spasi -> NULL
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(
            lambda v: None if isinstance(v, str) and v.strip() == "" else v
        )
        # Kolom yang seluruhnya kosong: paksa tipe string,
        # supaya BigQuery tidak gagal menebak tipe kolom
        if df[col].isna().all():
            df[col] = df[col].astype("string")

    # 3. LOAD
    bq_hook = BigQueryHook(gcp_conn_id="2", use_legacy_sql=False)
    client = bq_hook.get_client(project_id=PROJECT_ID)
    destination = f"{PROJECT_ID}.{DATASET_ID}.{bq_table}"

    client.delete_table(destination, not_found_ok=True)

    job = client.load_table_from_dataframe(
        df,
        destination,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"{len(df)} baris dimuat ke {destination}")


with DAG(
    dag_id="postgres_to_bigquery_full_etl_V8",
    default_args=default_args,
    description="ETL lengkap dari PostgreSQL MyPorto ke BigQuery",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "postgres", "bigquery", "myporto"],
) as dag:

    @task(task_id="etl_users")
    def task_users():
        run_etl_table("Users", "dim_users")

    @task(task_id="etl_certificates")
    def task_certificates():
        run_etl_table("Certificates", "fact_certificates")

    @task(task_id="etl_educations")
    def task_educations():
        run_etl_table("Educations", "fact_educations")

    @task(task_id="etl_experiences")
    def task_experiences():
        run_etl_table("Experiences", "fact_experiences")

    @task(task_id="etl_projects")
    def task_projects():
        run_etl_table("Projects", "fact_projects")

    @task(task_id="etl_about")
    def task_about():
        run_etl_table("About", "dim_about")

    @task(task_id="etl_jobdesk")
    def task_jobdesk():
        run_etl_table("Jobdesk", "dim_jobdesk")

    @task(task_id="etl_techstack")
    def task_techstack():
        run_etl_table("TechStack", "dim_techstack")

    task_users() >> [
        task_certificates(),
        task_educations(),
        task_experiences(),
        task_projects(),
        task_about(),
        task_jobdesk(),
        task_techstack(),
    ]