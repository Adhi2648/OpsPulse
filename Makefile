PYTHON ?= python

.PHONY: bootstrap generate-data etl etl-dry api test test-integration

bootstrap:
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install pytest pydantic-settings httpx

generate-data:
	$(PYTHON) scripts/generate_workflow_data.py --records 500000

etl:
	$(PYTHON) -m opspulse.etl.pipeline --input data/raw/workflow_events.csv

etl-dry:
	$(PYTHON) -m opspulse.etl.pipeline --input data/raw/workflow_events.csv --dry-run

api:
	$(PYTHON) -m opspulse.api

test:
	$(PYTHON) -m pytest tests -k "not integration"

test-integration:
	$(PYTHON) -m pytest tests/integration -m integration
