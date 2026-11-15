from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_dag_dari_git",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["contoh"],
)
def hello_dag():
    @task
    def say_hello():
        print("Halo dari Airflow di Hugging Face!")
        return "ok"

    @task
    def say_done(hasil: str):
        print(f"Task sebelumnya: {hasil}")

    say_done(say_hello())


hello_dag()