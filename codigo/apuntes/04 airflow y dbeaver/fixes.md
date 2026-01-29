Working recipe that fixed your stack. Commands are PowerShell-friendly.

# What broke

1. Wrong image tag in Compose → services kept running `apache/airflow:3.1.1`.
2. Provider path not visible by Python.
3. `PostgresOperator` removed in provider ≥6.x.
4. Airflow connection had `conn_type="postgresql+psycopg2"` instead of `"postgres"`.

# Target state

* All Airflow services use your custom image.
* Provider packages importable.
* DAG uses `SQLExecuteQueryOperator`.
* `postgres_default` connection has `conn_type=postgres`.

# Minimal file edits

**docker-compose.yaml** (shared env block)

```yaml
image: ${AIRFLOW_IMAGE_NAME:-extending-airflow:3.1.1}

# Make user-site visible to Python
PYTHONPATH: /home/airflow/.local/lib/python3.12/site-packages

# Boot-time install safety net (can remove later if baked into image)
_PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-postgres psycopg2-binary ${_PIP_ADDITIONAL_REQUIREMENTS:-}"

# Define task connection
AIRFLOW_CONN_POSTGRES_DEFAULT: postgres://airflow:airflow@postgres:5432/airflow
```

**DAG import change**

```python
# replace removed operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Postgres hook is still here if needed
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

# Rebuild or retag image (if needed)

```powershell
docker build --no-cache -t extending-airflow:3.1.1 .
# optional if compose expects another tag:
# docker tag extending-airflow:3.1.1 extending_airflow:3.1.1
```

# Clean start order

```powershell
docker compose down --remove-orphans
docker compose up -d postgres redis
docker compose ps          # wait for both Healthy
docker compose up -d airflow-init
docker compose logs -f airflow-init   # wait until exits 0
docker compose up -d airflow-apiserver airflow-dag-processor airflow-scheduler airflow-triggerer airflow-worker
docker compose ps
```

# Verifications

```powershell
# All services must use your image
docker ps --format 'table {{.Names}}\t{{.Image}}'

# Operator import inside containers
docker compose exec -it airflow-scheduler bash -lc 'python -c "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator; print(\"import OK\")"'
docker compose exec -it airflow-dag-processor bash -lc 'python -c "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator; print(\"import OK\")"'

# Connection exists and has correct type
docker compose exec -it airflow-scheduler bash -lc 'airflow connections get postgres_default | sed -n "1,12p"'
```

# Run the DAG and inspect

```powershell
# optional: check for leftover import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# list and trigger
docker compose exec airflow-scheduler airflow dags list | Select-String -Pattern postgres_sql_execute_example
docker compose exec airflow-scheduler airflow dags trigger postgres_sql_execute_example

# verify data in Postgres
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM public.pet;"
```

# Optional “bake-in” Dockerfile (instead of boot-time installs)

```dockerfile
FROM apache/airflow:3.1.1
USER airflow
ARG AIRFLOW_VERSION=3.1.1
ARG PY_VER=3.12
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt --constraint "${CONSTRAINTS_URL}"
ENV PYTHONPATH=/home/airflow/.local/lib/python${PY_VER}/site-packages
```

`requirements.txt` minimal:

```
apache-airflow-providers-postgres
psycopg2-binary
numpy
pandas
openpyxl
requests
beautifulsoup4
lxml
yfinance==0.2.66
```

# Pitfalls 

* Image tag mismatch (`extending-airflow` vs `apache/airflow`).
* PowerShell quoting breaking inline Python → use `bash -lc 'python -c "…"'` or open an interactive shell.
* Removed operator (`PostgresOperator`) → use `SQLExecuteQueryOperator`.
* Wrong connection scheme → `postgres://…` sets `conn_type=postgres`.
