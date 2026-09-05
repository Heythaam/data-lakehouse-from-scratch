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

benchmark-spark:
	docker compose exec -T -u 0 -e HOME=/tmp spark-master bash -c "pip install --quiet pandas python-dotenv && spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 /app/benchmark_spark.py"

benchmark-all:
	python src/benchmark_comparison.py

test:
	pytest tests/ -v