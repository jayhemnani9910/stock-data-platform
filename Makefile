.PHONY: up down build rebuild logs ps test lint migrate export-dashboard clean

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

rebuild:
	docker compose down
	docker compose build
	docker compose up -d

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .

# SQL/schema.sql only runs on a first container init, so schema changes reach a
# live volume through here. Every migration is idempotent -- re-running the set
# is a no-op.
migrate:
	@set -e; for f in SQL/migrations/*.sql; do \
		echo "applying $$f"; \
		docker exec -i timescaledb psql -v ON_ERROR_STOP=1 \
			-U "$${DB_USER:-data226}" -d "$${DB_NAME:-stockdw}" < "$$f"; \
	done

# Rewrites site/data/*.json from the warehouse. Those files are tracked because
# Pages serves them, so commit the result -- nothing regenerates them in CI.
export-dashboard:
	python3 scripts/export_dashboard_data.py

# Destroys the warehouse. `down -v` removes volumes and --rmi drops the built
# images; the database itself is the ./data/db bind mount, which this does NOT
# remove -- delete that by hand if you really mean it.
clean:
	docker compose down -v --rmi local
