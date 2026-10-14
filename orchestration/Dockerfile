# Airflow on Python 3.12 (ADR-028), dbt and DVT in venvs of their own
ARG AIRFLOW_IMAGE_TAG=3.3.2-python3.12
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

# Pinning apache-airflow in the same command stops pip moving Airflow to fit our packages
COPY orchestration/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt \
    && pip check

# dbt runs as a subprocess: its pins clash with Airflow's
COPY dbt/requirements.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/venv-dbt \
    && /opt/airflow/venv-dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# DVT pins numpy below 2 and pyarrow below 15 (ADR-025)
COPY validation/requirements.txt /tmp/requirements-dvt.txt
RUN python -m venv /opt/airflow/venv-dvt \
    && /opt/airflow/venv-dvt/bin/pip install --no-cache-dir -r /tmp/requirements-dvt.txt
