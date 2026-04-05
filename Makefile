.PHONY: up down build rebuild logs ps clean

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

clean:
	docker compose down -v --rmi local
