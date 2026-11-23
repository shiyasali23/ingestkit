PYTHON ?= python3
CONFIG ?= pipeline_config.yaml

.PHONY: install test lint format sample-data plan run resume status dev-up dev-down image tf-check

install:
	$(PYTHON) -m pip install -e ".[dev,postgres,kubernetes]"

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check ingestkit tests scripts airflow_dags

format:
	$(PYTHON) -m ruff format ingestkit tests scripts airflow_dags

sample-data:
	$(PYTHON) scripts/generate_sample_files.py --output-path ./sample_data/lab_upload --case-count 20

plan:
	ingestkit plan --config $(CONFIG)

run:
	ingestkit run --config $(CONFIG)

resume:
	ingestkit resume --config $(CONFIG) --run-id $(RUN_ID)

status:
	ingestkit status --config $(CONFIG)

dev-up:
	docker compose up -d

dev-down:
	docker compose down

image:
	docker build -t ingestkit-worker:latest .

tf-check:
	cd terraform && terraform fmt -check && terraform init -backend=false && terraform validate
