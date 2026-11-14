FROM apache/airflow:slim-3.4.0

# HF Spaces menjalankan container dengan UID 1000
USER root
RUN usermod -u 1000 airflow \
 && mkdir -p /opt/airflow/dags \
 && chown -R airflow:0 /opt/airflow /home/airflow

USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    apache-airflow-providers-standard

COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 start.sh /opt/airflow/start.sh

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__API__PORT=7860 \
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:7860/execution/ \
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin \
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/passwords.json

EXPOSE 7860
ENTRYPOINT ["bash", "/opt/airflow/start.sh"]