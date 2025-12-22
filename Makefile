.PHONY: ingest lint format check

ingest:
	poetry run python app/main.py

lint:
	poetry run ruff check . --fix

format:
	poetry run ruff format .

check: lint format
