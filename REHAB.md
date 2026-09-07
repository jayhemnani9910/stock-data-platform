# Run notes

How to exercise this project, and what must never be run automatically.

## What it is

A containerised data pipeline. Seven Docker services: TimescaleDB (star-schema
warehouse), Airflow webserver and scheduler, Zookeeper, Kafka, and a Kafka
producer/consumer pair. Ten Airflow DAG groups load prices, fundamentals,
earnings, SEC filings and FRED macro series. A static site under `site/` reads
JSON exported from the warehouse.

## Prerequisites

- Docker and Docker Compose.
- A `.env` file in the repo root. Copy `.env.example` and fill it in.
  `DB_PASSWORD`, `POSTGRES_PASSWORD`, `AIRFLOW__WEBSERVER__SECRET_KEY` and
  `AIRFLOW__CORE__FERNET_KEY` have no defaults and the stack refuses to start
  without them.
- `FRED_API_KEY` is needed by `macro_daily` only. `EDGAR_IDENTITY` is needed by
  `sec_financials_quarterly` only.

## Host port note

`docker-compose.yml` publishes TimescaleDB on host port 5434. On a machine where
something else already holds 5434, pass an override rather than editing the
committed file:

    cat > /tmp/override.yml <<'EOF'
    services:
      timescaledb:
        ports: !override
          - "5435:5432"
    EOF
    docker compose -f docker-compose.yml -f /tmp/override.yml up -d

The `!override` tag is required. A plain merge appends to the ports list instead
of replacing it, so both ports get published and the bind still fails.

## Run command

    docker compose up -d

Then wait for Airflow, and check the warehouse:

    curl -s http://localhost:8081/health
    docker exec timescaledb psql -U data226 -d stockdw -c \
      "SELECT count(*), max(date) FROM fact_stock_price_daily;"

Airflow UI is at http://localhost:8081 with admin/admin.

On a first run, the dimension DAGs must go first: `populate_dim_company`, then
`populate_dim_date`, then any `etl_stock_data_<ticker>`.

## Tests

    pytest tests/ -q

62 unit tests, no database or network needed. This is what CI runs, along with
`ruff check .` and `ruff format --check .`.

## Safe to run

- `pytest tests/ -q`
- `ruff check .`, `ruff format --check .`
- `docker compose up -d`, `docker compose ps`, `docker compose logs`
- `docker exec timescaledb psql ...` for any SELECT
- `scripts/export_dashboard_data.py`, which only reads the warehouse and
  rewrites `site/data/*.json`
- Triggering any Airflow DAG. They all upsert, so a repeat run is idempotent.

## Do not run automatically

- `docker compose down -v` and `make clean`. Both take the `-v` flag and destroy
  the warehouse volume, which is six months of loaded data.
- Anything that writes to `.env`. It holds a live FRED API key.
- `sec_financials_quarterly` in a tight loop. SEC EDGAR rate-limits by the
  `EDGAR_IDENTITY` string and will block it.
- `git push` from an audit run.

## Known slow steps

A first-run `etl_stock_data_<ticker>` pulls 25 years from yfinance and takes a
minute or so per ticker. `sec_financials_quarterly` walks EDGAR filings for ten
companies and is the slowest DAG in the project.
