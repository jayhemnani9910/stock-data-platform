# Run notes

How to exercise this project, and what must never be run automatically.

## What it is

A containerised data pipeline. Seven Docker services: TimescaleDB (star-schema
warehouse), Airflow webserver and scheduler, Zookeeper, Kafka, and a Kafka
producer/consumer pair. Airflow DAGs load daily prices, hourly intraday bars,
fundamentals, earnings, SEC filings and FRED macro series. A static site under `site/` reads
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

`airflow` and `stockdw` are separate databases on the same server: Airflow's
run history and users are in `airflow`, and `stockdw` holds only the nine
star-schema tables. A volume created before that split has 42 Airflow tables
sitting in `stockdw.public`; `CREATE DATABASE airflow` and restart, and Airflow
rebuilds its own schema (past run history does not carry over).

Then wait for Airflow, and check the warehouse:

    curl -s http://localhost:8081/health
    docker exec timescaledb psql -U data226 -d stockdw -c \
      "SELECT count(*), max(date) FROM fact_stock_price_daily;"

Airflow UI is at http://localhost:8081, credentials from `AIRFLOW_ADMIN_USER`
and `AIRFLOW_ADMIN_PASSWORD` in `.env`.

On a first run, the dimension DAGs must go first: `populate_dim_company`, then
`populate_dim_date`, then any `etl_stock_data_<ticker>`.

## Tests

    pytest tests/ -q

181 unit tests, no database or network needed. This is what CI runs, along with
`ruff check .` and `ruff format --check .`.

They import the real production functions. They used to run against copies, and
stayed green while `_stage_path_for_run`, `load_tickers`, `MARKET_OPEN` and the
`dim_date` weekend rule were all replaced with nonsense. If you change the test
suite, gut a function and confirm the suite goes red before believing it.

## Safe to run

- `pytest tests/ -q`
- `ruff check .`, `ruff format --check .`
- `docker compose up -d`, `docker compose ps`, `docker compose logs`
- `docker exec timescaledb psql ...` for any SELECT
- `make export-dashboard`, which only reads the warehouse and rewrites
  `site/data/*.json`. Those files are tracked, so commit the result — nothing
  regenerates them in CI, and Pages serves whatever is committed.
- `make migrate`, which applies `SQL/migrations/*.sql`. Every migration is
  re-runnable; applying the set twice is a no-op.
- Triggering any Airflow DAG. They all upsert, so a repeat run is idempotent.

## Do not run automatically

- `docker compose down -v` and `make clean`. Both take the `-v` flag. The
  database itself is the `./data/db` bind mount, which `-v` does not remove —
  but do not rely on that.
- `TRUNCATE fact_stock_price_daily` followed by the ETL DAGs. That is the only
  way to clear a stale price-adjustment basis, and it refetches every bar
  back to 1962 for ten tickers. It is correct, it is slow, and it is not something to do
  unattended.
- Anything that writes to `.env`. It holds a live FRED API key.
- `sec_financials_quarterly` in a tight loop. SEC EDGAR rate-limits by the
  `EDGAR_IDENTITY` string and will block it.
- `git push` from an audit run.

## Known slow steps

A first-run `etl_stock_data_<ticker>` pulls the ticker's entire history from
yfinance -- back to 1962 for DIS, 1980 for AAPL and JPM -- and takes a minute
or so per ticker. The same happens on any run where a dividend or split
has gone ex since the last load — that is deliberate: it re-adjusts the whole
series so the history does not step at the boundary.

`sec_financials_quarterly` walks EDGAR filings for ten companies and is the
slowest DAG in the project.
