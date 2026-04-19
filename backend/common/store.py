"""
Sync MongoDB store for Celery workers (PyMongo).
"""

from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING, DESCENDING

from .config import settings
from .models import DatasetEntity, DatasetStatus


_client: MongoClient | None = None


def _get_collection():
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGO_URL)
    return _client[settings.MONGO_DB][settings.MONGO_COLLECTION]


def reset_client() -> None:
    """Drop the cached client. Used after process fork in Celery workers."""
    global _client
    _client = None


def ensure_indexes() -> None:
    """Create the indexes the app relies on. Idempotent."""
    col = _get_collection()
    col.create_index([("dataset_id", ASCENDING)], unique=True, name="uniq_dataset_id")
    col.create_index([("created_at", DESCENDING)], name="created_at_desc")
    col.create_index([("status", ASCENDING)], name="status")


def create_dataset(entity: DatasetEntity) -> None:
    col = _get_collection()
    col.insert_one(entity.to_dict())


def update_dataset_status(
    dataset_id: str,
    status: DatasetStatus,
) -> None:
    col = _get_collection()
    update = {
        "$set": {
            "status": status.value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    }
    col.update_one({"dataset_id": dataset_id}, update)


def set_dataset_result(dataset_id: str, result: dict) -> None:
    col = _get_collection()
    col.update_one(
        {"dataset_id": dataset_id},
        {
            "$set": {
                "status": DatasetStatus.COMPLETED.value,
                "result": result,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )


def set_dataset_failed(dataset_id: str, error: str) -> None:
    col = _get_collection()
    col.update_one(
        {"dataset_id": dataset_id},
        {
            "$set": {
                "status": DatasetStatus.FAILED.value,
                "error": error,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )


def get_dataset(dataset_id: str) -> DatasetEntity | None:
    col = _get_collection()
    doc = col.find_one({"dataset_id": dataset_id}, {"_id": 0})
    if doc is None:
        return None
    return DatasetEntity.from_dict(doc)


def list_datasets() -> list[dict]:
    col = _get_collection()
    docs = col.find({}, {"_id": 0}).sort("created_at", -1)
    return list(docs)
