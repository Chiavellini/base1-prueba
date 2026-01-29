FROM apache/airflow:3.1.1
USER airflow

ARG AIRFLOW_VERSION=3.1.1
ARG PY_VER=3.12
# ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user --upgrade pip \
 && pip install --no-cache-dir --user -r /requirements.txt 
 
# --constraint "${CONSTRAINTS_URL}"



# keep Airflow's default entrypoint/cmd


# Keep Airflow’s default entrypoint/cmd from the base image
# Example build:-
#   docker compose down
#   docker build -t extending-airflow:3.1.1 .
#   docker compose up -d --force-recreate

# the libraries i installed
# pip install numpy pandas yfinance openpyxl