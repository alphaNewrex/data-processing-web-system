"""
Async MongoDB store for FastAPI (PyMongo Async).
"""

from pymongo import AsyncMongoClient, ASCENDING, DESCENDING

from common.config import settings
from common.models import DatasetEntity, TERMINAL_STATUSES

_client: AsyncMongoClient | None = None


def get_async_client() -> AsyncMongoClient:
    global _client
    if _client is None:
        _client = AsyncMongoClient(settings.MONGO_URL)
    return _client


def _get_collection():
    client = get_async_client()
    return client[settings.MONGO_DB][settings.MONGO_COLLECTION]


async def ensure_indexes() -> None:
    """Create indexes on startup. Idempotent."""
    col = _get_collection()
    await col.create_index([("dataset_id", ASCENDING)], unique=True, name="uniq_dataset_id")
    await col.create_index([("created_at", DESCENDING)], name="created_at_desc")
    await col.create_index([("status", ASCENDING)], name="status")


async def create_dataset(entity: DatasetEntity) -> None:
    col = _get_collection()
    await col.insert_one(entity.to_dict())


async def get_dataset(dataset_id: str) -> dict | None:
    col = _get_collection()
    doc = await col.find_one({"dataset_id": dataset_id}, {"_id": 0})
    return doc


async def set_celery_task_id(dataset_id: str, task_id: str) -> None:
    """Record the Celery chain's task id so operators can trace it later."""
    col = _get_collection()
    await col.update_one(
        {"dataset_id": dataset_id},
        {"$set": {"celery_task_id": task_id}},
    )


async def delete_dataset(dataset_id: str) -> bool:
    col = _get_collection()
    result = await col.delete_one({"dataset_id": dataset_id})
    return result.deleted_count > 0


async def list_terminal_dataset_ids() -> list[str]:
    """IDs of datasets in a terminal state (COMPLETED / FAILED)."""
    col = _get_collection()
    cursor = col.find(
        {"status": {"$in": list(TERMINAL_STATUSES)}},
        {"_id": 0, "dataset_id": 1},
    )
    docs = await cursor.to_list(length=None)
    return [d["dataset_id"] for d in docs]


async def delete_datasets_by_ids(ids: list[str]) -> int:
    """Hard-delete the given dataset rows. Returns the number removed."""
    if not ids:
        return 0
    col = _get_collection()
    result = await col.delete_many({"dataset_id": {"$in": ids}})
    return result.deleted_count


async def delete_all_datasets() -> list[str]:
    """
    Delete all datasets whose status is COMPLETED or FAILED.

    Kept as a convenience wrapper; new callers should use
    `list_terminal_dataset_ids` + external cleanup + `delete_datasets_by_ids`
    to ensure object-store cleanup happens BEFORE Mongo rows are removed.
    """
    ids = await list_terminal_dataset_ids()
    await delete_datasets_by_ids(ids)
    return ids


async def list_datasets() -> list[dict]:
    col = _get_collection()
    cursor = col.find({}, {"_id": 0}).sort("created_at", -1)
    return await cursor.to_list(length=1000)
