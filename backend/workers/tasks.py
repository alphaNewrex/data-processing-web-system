"""
Celery tasks for dataset processing pipeline.

Chain: preprocess -> compute -> summarise

Only the dataset_id flows through the Celery broker. Each stage reads
its input payload from S3 (MinIO) and writes its output back, keeping
RabbitMQ messages tiny and making every stage individually replayable.

Each stage updates the Dataset entity in MongoDB with current status.
Uses acks_late + reject_on_worker_lost for crash resilience — if a worker
dies mid-task, the message is re-queued and picked up by another worker.
"""

import time
from datetime import datetime

from celery.utils.log import get_task_logger

from workers.celery_app import celery_app
from common.store import update_dataset_status, set_dataset_result, set_dataset_failed
from common.models import DatasetStatus
from common.storage import (
    get_json,
    put_json,
    KEY_RAW,
    KEY_PREPROCESSED,
    KEY_COMPUTED,
    KEY_RESULT,
)

logger = get_task_logger(__name__)


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
def preprocess(self, dataset_id: str) -> str:
    """
    Read raw.json from storage, validate records, write preprocessed.json.
    Updates entity status to PREPROCESSING. Returns dataset_id.
    """
    logger.info("[PREPROCESS][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.PREPROCESSING)

    # Simulate long-running preprocessing
    time.sleep(5)

    data = get_json(dataset_id, KEY_RAW)
    records = data.get("records", [])
    record_count = len(records)

    valid_records = []
    invalid_count = 0

    for record in records:
        if _validate_record(record):
            valid_records.append(record)
        else:
            invalid_count += 1

    put_json(
        dataset_id,
        KEY_PREPROCESSED,
        {
            "dataset_id": dataset_id,
            "record_count": record_count,
            "valid_records": valid_records,
            "invalid_count": invalid_count,
        },
    )
    logger.info(
        "[PREPROCESS][DONE] dataset=%s total=%d valid=%d invalid=%d",
        dataset_id, record_count, len(valid_records), invalid_count,
    )
    return dataset_id


@celery_app.task(bind=True, name="tasks.compute")
def compute(self, dataset_id: str) -> str:
    """
    Read preprocessed.json, compute summary, write computed.json.
    Simulates long-running computation with 15s delay.
    Updates entity status to COMPUTING. Returns dataset_id.
    """
    logger.info("[COMPUTE][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.COMPUTING)

    prev = get_json(dataset_id, KEY_PREPROCESSED)
    valid_records = prev["valid_records"]

    # Simulate long-running computation
    time.sleep(5)

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

    put_json(
        dataset_id,
        KEY_COMPUTED,
        {
            "dataset_id": dataset_id,
            "record_count": prev["record_count"],
            "invalid_count": prev["invalid_count"],
            "category_summary": category_summary,
            "average_value": average_value,
        },
    )
    logger.info(
        "[COMPUTE][DONE] dataset=%s categories=%d avg=%.2f",
        dataset_id, len(category_summary), average_value,
    )
    return dataset_id


@celery_app.task(bind=True, name="tasks.summarise")
def summarise(self, dataset_id: str) -> dict:
    """
    Read computed.json, write result.json and persist summary to MongoDB.
    Updates entity status to SUMMARISING, then COMPLETED.
    """
    logger.info("[SUMMARISE][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.SUMMARISING)

    # Simulate long-running summarisation
    time.sleep(5)

    prev = get_json(dataset_id, KEY_COMPUTED)

    result = {
        "dataset_id": dataset_id,
        "record_count": prev["record_count"],
        "category_summary": prev["category_summary"],
        "average_value": prev["average_value"],
        "invalid_records": prev["invalid_count"],
    }

    put_json(dataset_id, KEY_RESULT, result)
    set_dataset_result(dataset_id, result)
    logger.info("[SUMMARISE][DONE] dataset=%s status=COMPLETED", dataset_id)
    return result


@celery_app.task(bind=True, name="tasks.on_pipeline_error")
def on_pipeline_error(self, request, exc, traceback, dataset_id: str) -> None:
    """Error callback: mark the dataset as FAILED in MongoDB."""
    logger.error("[PIPELINE][FAILED] dataset=%s error=%s", dataset_id, exc)
    set_dataset_failed(dataset_id, str(exc))
