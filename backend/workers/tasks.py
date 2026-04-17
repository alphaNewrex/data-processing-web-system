"""
Celery tasks for dataset processing pipeline.

Chain: preprocess -> compute -> summarise

Each stage updates the Dataset entity in MongoDB with current status.
Uses acks_late + reject_on_worker_lost for crash resilience — if a worker
dies mid-task, the message is re-queued and picked up by another worker.
"""

import time
from datetime import datetime

from workers.celery_app import celery_app
from common.store import update_dataset_status, set_dataset_result, set_dataset_failed
from common.models import DatasetStatus


def _validate_record(record: dict) -> bool:
    """Check if a record has all required fields with valid types."""
    # Required fields: id, timestamp, value, category
    if not isinstance(record, dict):
        return False

    required = ["id", "timestamp", "value", "category"]
    for field in required:
        if field not in record or record[field] is None:
            return False

    # Validate timestamp is a parseable ISO string
    try:
        datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return False

    # Validate value is numeric
    if not isinstance(record["value"], (int, float)):
        return False

    # Validate category is a non-empty string
    if not isinstance(record["category"], str) or not record["category"].strip():
        return False

    return True


@celery_app.task(bind=True, name="tasks.preprocess")
def preprocess(self, dataset_id: str, data: dict) -> dict:
    """
    Validate records, separate valid from invalid.
    Updates entity status to PREPROCESSING.
    """
    update_dataset_status(dataset_id, DatasetStatus.PREPROCESSING)

    # Simulate long-running preprocessing
    time.sleep(15)

    records = data.get("records", [])
    record_count = len(records)

    valid_records = []
    invalid_count = 0

    for record in records:
        if _validate_record(record):
            valid_records.append(record)
        else:
            invalid_count += 1

    return {
        "dataset_id": dataset_id,
        "record_count": record_count,
        "valid_records": valid_records,
        "invalid_count": invalid_count,
    }


@celery_app.task(bind=True, name="tasks.compute")
def compute(self, prev_result: dict) -> dict:
    """
    Compute category_summary and average_value.
    Simulates long-running computation with 15s delay.
    Updates entity status to COMPUTING.
    """
    dataset_id = prev_result["dataset_id"]
    update_dataset_status(dataset_id, DatasetStatus.COMPUTING)

    valid_records = prev_result["valid_records"]

    # Simulate long-running computation
    time.sleep(15)

    # Build category summary
    category_summary: dict[str, int] = {}
    for record in valid_records:
        cat = record["category"]
        category_summary[cat] = category_summary.get(cat, 0) + 1

    # Compute average value
    if valid_records:
        total_value = sum(r["value"] for r in valid_records)
        average_value = round(total_value / len(valid_records), 2)
    else:
        average_value = 0.0

    return {
        "dataset_id": dataset_id,
        "record_count": prev_result["record_count"],
        "invalid_count": prev_result["invalid_count"],
        "category_summary": category_summary,
        "average_value": average_value,
    }


@celery_app.task(bind=True, name="tasks.summarise")
def summarise(self, prev_result: dict) -> dict:
    """
    Assemble final output and persist result to MongoDB.
    Updates entity status to SUMMARISING, then COMPLETED.
    """
    dataset_id = prev_result["dataset_id"]
    update_dataset_status(dataset_id, DatasetStatus.SUMMARISING)

    # Simulate long-running summarisation
    time.sleep(15)

    result = {
        "dataset_id": dataset_id,
        "record_count": prev_result["record_count"],
        "category_summary": prev_result["category_summary"],
        "average_value": prev_result["average_value"],
        "invalid_records": prev_result["invalid_count"],
    }

    set_dataset_result(dataset_id, result)
    return result


@celery_app.task(bind=True, name="tasks.on_pipeline_error")
def on_pipeline_error(self, request, exc, traceback, dataset_id: str) -> None:
    """Error callback: mark the dataset as FAILED in MongoDB."""
    set_dataset_failed(dataset_id, str(exc))
