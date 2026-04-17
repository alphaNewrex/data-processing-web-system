"""
Async MongoDB store for FastAPI (Motor).
"""

from motor.motor_asyncio import AsyncIOMotorClient

from common.config import settings
from common.models import DatasetEntity

_client: AsyncIOMotorClient | None = None


def get_motor_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(settings.MONGO_URL)
    return _client


def _get_collection():
    client = get_motor_client()
    return client[settings.MONGO_DB][settings.MONGO_COLLECTION]


async def create_dataset(entity: DatasetEntity) -> None:
    col = _get_collection()
    await col.insert_one(entity.to_dict())


async def get_dataset(dataset_id: str) -> dict | None:
    col = _get_collection()
    doc = await col.find_one({"dataset_id": dataset_id}, {"_id": 0})
    return doc


async def delete_dataset(dataset_id: str) -> bool:
    col = _get_collection()
    result = await col.delete_one({"dataset_id": dataset_id})
    return result.deleted_count > 0


async def delete_all_datasets() -> int:
    col = _get_collection()
    result = await col.delete_many({"status": {"$in": ["COMPLETED", "FAILED"]}})
    return result.deleted_count


async def list_datasets() -> list[dict]:
    col = _get_collection()
    cursor = col.find({}, {"_id": 0}).sort("created_at", -1)
    return await cursor.to_list(length=1000)
