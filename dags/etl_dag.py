from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from relation_etl.extract import extract_all
from relation_etl.load import load
from relation_etl.transform import QuarantineRecord, TransformResult, transform

MOCK_API_BASE_URL = os.environ.get(
    "MOCK_API_BASE_URL", "http://host.docker.internal:8000"
)
WAREHOUSE_DB_PATH = "/opt/airflow/warehouse/warehouse.db"


@dag(
    dag_id="relation_etl",
    description="Extract genes/transcripts/exons from the mock API, clean, load to SQLite",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "genomics"],
)
def relation_etl_dag():
    @task(retries=0)  # extract.py already retries transient 429/500;
    def run_extract():
        return extract_all(MOCK_API_BASE_URL)

    @task
    def run_transform(raw: dict) -> dict:
        result = transform(raw)
        return {
            "genes": result.genes,
            "transcripts": result.transcripts,
            "exons": result.exons,
            "quarantine": [
                {"table": q.table, "record": q.record, "reason": q.reason}
                for q in result.quarantine
            ],
            "merge_log": result.merge_log,
        }

    @task
    def run_load(transformed: dict) -> str:
        result = TransformResult(
            genes=transformed["genes"],
            transcripts=transformed["transcripts"],
            exons=transformed["exons"],
            quarantine=[QuarantineRecord(**q) for q in transformed["quarantine"]],
            merge_log=transformed["merge_log"],
        )
        run_id = load(result, WAREHOUSE_DB_PATH)
        return run_id

    run_load(run_transform(run_extract()))


relation_etl_dag()
