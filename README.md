# Production ETL Pipeline | NYC Taxi Trips

A production-grade, multi-stage ETL pipeline over the public NYC TLC yellow-taxi trip dataset, orchestrated **two ways** (Apache Airflow and Dagster) on top of the same shared Python core. The pipeline ingests monthly Parquet files, validates them against a strict schema, cleans the data, lands it in a Postgres warehouse organised into raw / staging / marts layers, and runs multi-layer data-quality gates plus row-count anomaly detection on every run. Failures and anomalies page oncall through a configurable Slack alert sender.

## Highlights

The same business logic, schema contract, and quality suite drive both orchestrators, so swapping schedulers does not change behaviour. Every stage carries automated retries with exponential backoff, structured logging, idempotent loads keyed by `(trip_year, trip_month)`, an `on_failure_callback` that pages oncall, and a separate monitoring DAG/sensor that scans recent partitions for silent regressions. Schema enforcement is implemented with Pandera at the boundary of every stage. Row-count anomaly detection compares each partition against the trailing-six-month median and refuses to publish if the deviation exceeds a configurable threshold or if the absolute count drops below an operator-set floor.

## Architecture

```
                 +-------------------+
                 |  NYC TLC Parquet  |
                 +---------+---------+
                           |
                           v
+---------+   +-----------+-----------+   +-------------+
| extract +-->+ validate_raw_schema   +-->+ load_staging|
+---------+   +-----------+-----------+   +------+------+
                                                 |
                                                 v
                                +----------------+----------------+
                                |        quality gates           |
                                |  - dataframe suite             |
                                |  - warehouse suite             |
                                |  - row-count anomaly detector  |
                                +----------------+----------------+
                                                 |
                                                 v
                                  +--------------+--------------+
                                  |   transform_marts (SQL)     |
                                  |  - dim_zone upsert          |
                                  |  - fct_daily_trips refresh  |
                                  +--------------+--------------+
                                                 |
                                                 v
                                       +---------+---------+
                                       | record_pipeline_run|
                                       +-------------------+
```

The repository is laid out as follows:

```
nyc-taxi-etl/
  shared/              # business logic shared by both orchestrators
    config.py          # env-driven Settings dataclass
    extract.py         # retried Parquet downloader (tenacity)
    transform.py       # pure-function cleaning rules
    schema.py          # Pandera schemas (raw + staging)
    quality_checks.py  # multi-layer quality suites
    anomaly.py         # row-count anomaly detector
    alerts.py          # Slack/log alert sender
    db.py              # SQLAlchemy + psycopg helpers
    load.py            # idempotent loaders & DDL bootstrap
  sql/
    ddl/               # raw / staging / marts / ops schemas + indexes
    transformations/   # parameterised SQL: dim_zone, fct_daily_trips
  airflow/
    dags/
      nyc_taxi_etl.py  # the production DAG
      monitoring.py    # daily anomaly scan DAG
    plugins/callbacks.py  # failure / retry / SLA-miss callbacks
    Dockerfile
  dagster_project/
    nyc_taxi/
      assets/          # raw, staging, marts, ops assets
      checks/          # asset checks wrapping the shared suites
      jobs.py
      sensors.py       # run-failure + run-health sensors
      resources.py     # warehouse, alert, settings resources
      partitions.py    # monthly partition definition
      definitions.py   # top-level Definitions object
    workspace.yaml
    Dockerfile
  tests/               # pytest unit suite (21 tests)
  .github/workflows/ci.yml
  docker-compose.yml   # postgres + airflow + dagster local stack
  requirements.txt     # full runtime deps
  requirements-ci.txt  # lean CI deps (no Airflow)
```

## Pipeline stages

The pipeline runs the same DAG of stages under both orchestrators.

The **init stage** applies every DDL file in `sql/ddl/` against the warehouse so the run is self-bootstrapping and safe to invoke against a fresh Postgres instance. The **extract stage** uses `tenacity` to retry transient HTTP failures with exponential backoff, writes to a `.part` temp file before atomic rename, and skips the download when the target file already exists with non-zero bytes (idempotent re-runs). The **schema-validation stage** drives a Pandera `DataFrameSchema` over the raw frame to catch upstream column drift before bad data lands in Postgres. The **load stage** cleans the frame, validates the staging schema, and replaces the `(trip_year, trip_month)` partition in `staging.fct_trips` so retries do not double-load. The **quality stage** runs three independent gates: a dataframe suite (null checks, fare sanity, pickup/dropoff ordering, distinct-zone coverage), a warehouse suite (row count above floor, trip_id uniqueness), and a row-count anomaly detector that compares the partition's row count against the trailing six-month median. The **transform stage** runs parameterised SQL files to upsert `marts.dim_zone` and refresh `marts.fct_daily_trips` for the partition. The **record stage** writes to `ops.pipeline_runs` for run history.

## Multi-layer quality validation

Every gate emits a structured `CheckResult` with `severity` (`error` or `warn`), the measured metric, the threshold, and a human-readable detail. Errors fail the pipeline. Warnings are logged and surface in the Dagster UI as soft check failures but do not block the run. Anomaly snapshots are written to `ops.row_count_history` so the monitoring DAG (Airflow) and the run-health sensor (Dagster) can backfill-check the last six partitions on a daily cadence and page oncall on silent regressions.

## Production controls

Both orchestrators enforce automated retries with exponential backoff (Airflow `retries=3`, `retry_exponential_backoff=True`; Dagster `RetryPolicy` with `Backoff.EXPONENTIAL` and `Jitter.PLUS_MINUS`). The Airflow DAG configures `on_failure_callback`, `on_retry_callback`, and an `sla_miss_callback`; Dagster pairs every job with a `run_failure_sensor` and a 36-hour run-health watchdog sensor. Alert payloads are dispatched through a single `shared.alerts.send_alert` sink that posts to Slack when `SLACK_WEBHOOK_URL` is set and always writes to the structured log so failures are visible in the orchestrator UI as well as in the alerting channel. Dependency management is enforced both at the Airflow task-graph level (`init >> extract_group >> load_group >> quality_group >> transform >> record`) and at the Dagster asset-graph level (`raw_trip_file -> cleaned_trips -> staging_trips_loaded -> {row_count_anomaly_recorded, dim_zone_refreshed} -> fct_daily_trips_refreshed`).

## Running the stack

Copy `.env.example` to `.env` and adjust as needed, then bring up the local stack:

```
cp .env.example .env
docker compose up -d
```

This starts a warehouse Postgres (`localhost:5432`), a separate Postgres for Airflow metadata, the Airflow webserver and scheduler (`http://localhost:8080`, login `admin/admin`), and the Dagster webserver and daemon (`http://localhost:3000`).

To trigger a one-off run for, say, March 2024 in Airflow, open the UI, find the `nyc_taxi_etl` DAG, click "Trigger DAG w/ config", and pass `{"year": 2024, "month": 3}`. In Dagster, open the asset graph, select the partition `2024-03-01`, and click "Materialize selected". Both runs land identical rows in `staging.fct_trips` and `marts.fct_daily_trips`.

## Tests and CI

Run the unit tests locally with:

```
pip install -r requirements-ci.txt
PYTHONPATH=. pytest -ra
```

The suite has 21 tests covering the pure-function transform logic, Pandera schema enforcement, the dataframe and warehouse quality checks, the row-count anomaly detector across all branches (below floor, large deviation, within threshold, no history), the alert sender (with and without a webhook), and a Dagster definitions smoke test that asserts every asset, job, schedule, and sensor loads.

The GitHub Actions workflow at `.github/workflows/ci.yml` runs ruff, black, and pytest on every push, and a separate job installs Airflow with the official constraints file and uses `DagBag` to verify that both DAGs parse without errors.

## Configuration knobs

Every behaviour above is environment-driven through `shared.config.Settings`. The most relevant knobs are `ROW_COUNT_DEVIATION_PCT` (default 0.30) for the relative anomaly threshold, `MIN_EXPECTED_ROWS` (default 10000) for the absolute floor, `SLACK_WEBHOOK_URL` for alert routing (alerts are log-only when blank), `TAXI_DATA_BASE_URL` for the source bucket, and the standard `POSTGRES_*` variables for the warehouse target.
