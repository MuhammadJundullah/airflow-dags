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


def run_etl_table(postgres_table: str, bq_table: str):
    # 1. EXTRACT dari PostgreSQL (Connection ID: '1')
    pg_hook = PostgresHook(postgres_conn_id="1")
    engine = pg_hook.get_sqlalchemy_engine()

    # Nama tabel dipakai apa adanya (case-sensitive), TANPA .lower()
    sql = f'SELECT * FROM public."{postgres_table}";'
    df = pd.read_sql(sql, engine)

    # 2. TRANSFORM: nama kolom jadi huruf kecil
    df.columns = [col.lower() for col in df.columns]

    # 3. LOAD ke BigQuery (Connection ID: '2')
    bq_hook = BigQueryHook(gcp_conn_id="2", use_legacy_sql=False)
    client = bq_hook.get_client(project_id=PROJECT_ID)
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{bq_table}"

    job = client.load_table_from_dataframe(
        df,
        destination_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"{len(df)} baris dimuat ke {destination_table}")


with DAG(
    dag_id="postgres_to_bigquery_full_etl_V3",
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

    task_users() >> [
        task_certificates(),
        task_educations(),
        task_experiences(),
        task_projects(),
    ]