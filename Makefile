up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

restart:
	docker compose down && docker compose up -d

status:
	docker compose ps

pipeline:
	python src/upload_to_minio.py
	python src/register_iceberg_table.py
	python src/load_full_year.py
	python src/benchmark.py

test:
	pytest tests/ -v