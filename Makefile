# Minimal Makefile — just enough to run your three commands

.PHONY: build up down

build:
	docker build -t airflow-spark-jars airflow/image-airflow-spark
	docker build -t spark-image spark
	docker build -t superset-trino superset

up: build
	docker compose up -d

down:
	docker compose down
