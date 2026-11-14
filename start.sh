#!/usr/bin/env bash
set -e

# Password login dari Secret (bukan hardcode)
echo "{\"admin\": \"${ADMIN_PASSWORD}\"}" > /opt/airflow/passwords.json

airflow db migrate
exec airflow standalone