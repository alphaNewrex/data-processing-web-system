"""
Celery tasks for dataset processing pipeline.

Chain: preprocess -> compute -> summarise

Only the dataset_id flows through the Celery broker. Each stage reads
its input payload from S3 (MinIO) and writes its output back, keeping
RabbitMQ messages tiny and making every stage individually replayable.

Each stage updates the Dataset entity in MongoDB with current status.
Uses acks_late + reject_on_worker_lost for crash resilience — if a worker
dies mid-task, the message is re-queued and picked up by another worker.

All pipeline tasks inherit from `DatasetTask`, which centralises:
  * retry policy for transient I/O errors (3 attempts, jittered backoff)
  * on_failure hook that marks the Dataset entity as FAILED in MongoDB

Adding a new stage is therefore a one-liner: decorate with
`@celery_app.task(base=DatasetTask, name=...)` and it inherits the
retry + failure semantics automatically.
"""

import time

from celery import Task
from celery.utils.log import get_task_logger

from workers.celery_app import celery_app
from common.config import settings
from common.store import update_dataset_status, set_dataset_result, set_dataset_failed
from common.models import DatasetStatus
from common.validation import is_valid_record
from common.storage import (
    get_json,
    put_json,
    delete_prefix,
    KEY_RAW,
    KEY_PREPROCESSED,
    KEY_COMPUTED,
    KEY_RESULT,
)

__all__ = ["DatasetTask", "preprocess", "compute", "summarise"]

logger = get_task_logger(__name__)


class DatasetTask(Task):
    """
    Base class for all pipeline tasks.

    Contract: the first positional argument MUST be `dataset_id: str`.
    This lets `on_failure` automatically mark the correct Dataset entity
    as FAILED without every task needing its own error callback.

    Retry policy is scoped to transient I/O errors — network blips, broker
    reconnects, storage timeouts. Business-logic exceptions (KeyError,
    ValueError, schema mismatches) fail fast so a bad dataset doesn't tie
    up a worker slot for 3 attempts.
    """

    # IOError is an alias for OSError in Py3; ConnectionError is an OSError
    # subclass. boto3's EndpointConnectionError also inherits from OSError,
    # so this covers MinIO/S3 connection failures.
    autoretry_for = (IOError, ConnectionError)
    max_retries = 3
    retry_backoff = True
    retry_backoff_max = 30
    retry_jitter = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):  # noqa: D401
        """Runs once retries are exhausted. Marks the dataset as FAILED
        and best-effort cleans up its object-store prefix so failed rows
        don't accumulate orphaned payloads."""
        dataset_id = kwargs.get("dataset_id") or (args[0] if args else None)
        if not dataset_id:
            # Shouldn't happen given the contract — log so we notice.
            logger.error("[PIPELINE][FAILED] task=%s no dataset_id in args", self.name)
            return

        logger.error("[PIPELINE][FAILED] dataset=%s error=%s", dataset_id, exc)
        set_dataset_failed(str(dataset_id), str(exc))
        try:
            delete_prefix(str(dataset_id))
        except Exception as cleanup_err:  # pragma: no cover - defensive
            # Don't mask the original failure — just log and move on.
            logger.warning(
                "[PIPELINE][FAILED][CLEANUP] dataset=%s error=%s",
                dataset_id, cleanup_err,
            )


def _simulate_work() -> None:
    """Artificial delay so stage transitions are observable in demos."""
    delay = settings.SIMULATE_DELAY_SECONDS
    if delay > 0:
        time.sleep(delay)


@celery_app.task(base=DatasetTask, name="tasks.preprocess")
def preprocess(dataset_id: str) -> str:
    """
    Read raw.json from storage, validate records, write preprocessed.json.
    Updates entity status to PREPROCESSING. Returns dataset_id.
    """
    logger.info("[PREPROCESS][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.PREPROCESSING)
    _simulate_work()

    data = get_json(dataset_id, KEY_RAW)
    records = data.get("records", [])
    record_count = len(records)

    valid_records = []
    invalid_count = 0

    for record in records:
        if is_valid_record(record):
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


@celery_app.task(base=DatasetTask, name="tasks.compute")
def compute(dataset_id: str) -> str:
    """
    Read preprocessed.json, compute category summary and average value,
    write computed.json. Updates entity status to COMPUTING. Returns dataset_id.
    """
    logger.info("[COMPUTE][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.COMPUTING)

    prev = get_json(dataset_id, KEY_PREPROCESSED)
    valid_records = prev.get("valid_records", [])
    _simulate_work()

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
            "record_count": prev.get("record_count", 0),
            "invalid_count": prev.get("invalid_count", 0),
            "category_summary": category_summary,
            "average_value": average_value,
        },
    )
    logger.info(
        "[COMPUTE][DONE] dataset=%s categories=%d avg=%.2f",
        dataset_id, len(category_summary), average_value,
    )
    return dataset_id


@celery_app.task(base=DatasetTask, name="tasks.summarise")
def summarise(dataset_id: str) -> dict:
    """
    Read computed.json, write result.json and persist summary to MongoDB.
    Updates entity status to SUMMARISING, then COMPLETED.
    """
    logger.info("[SUMMARISE][START] dataset=%s", dataset_id)
    update_dataset_status(dataset_id, DatasetStatus.SUMMARISING)
    _simulate_work()

    prev = get_json(dataset_id, KEY_COMPUTED)

    result = {
        "dataset_id": dataset_id,
        "record_count": prev.get("record_count", 0),
        "category_summary": prev.get("category_summary", {}),
        "average_value": prev.get("average_value", 0.0),
        "invalid_records": prev.get("invalid_count", 0),
    }

    put_json(dataset_id, KEY_RESULT, result)
    set_dataset_result(dataset_id, result)
    logger.info("[SUMMARISE][DONE] dataset=%s status=COMPLETED", dataset_id)
    return result



