import json
import logging

from fastapi import APIRouter, UploadFile, File, HTTPException
from pymongo.errors import DuplicateKeyError

from api.async_store import (
    create_dataset,
    get_dataset,
    list_datasets,
    delete_dataset,
    delete_datasets_by_ids,
    list_terminal_dataset_ids,
    set_celery_task_id,
)
from api.schemas import DatasetUploadResponse, DatasetStatusResponse
from common.models import DatasetEntity, DatasetStatus, TERMINAL_STATUSES
from common.storage import ensure_bucket, put_json, delete_prefix, KEY_RAW
from workers.workflow import build_processing_workflow

logger = logging.getLogger("api.routes")
router = APIRouter()


@router.post("/dataset", response_model=DatasetUploadResponse)
async def upload_dataset(file: UploadFile = File(...)):
    """
    Accept a JSON file upload (multipart/form-data), create a Dataset entity
    in MongoDB, and dispatch the processing pipeline to Celery.
    """
    # Validate file type
    if not file.filename or not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    # Parse JSON content
    try:
        content = await file.read()
        data = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON file: {e}")

    # Validate required fields in the JSON
    if "dataset_id" not in data:
        raise HTTPException(status_code=400, detail="JSON must contain 'dataset_id'")
    if "records" not in data or not isinstance(data["records"], list):
        raise HTTPException(status_code=400, detail="JSON must contain 'records' array")

    dataset_id = data["dataset_id"]
    record_count = len(data["records"])
    logger.info("[UPLOAD] dataset=%s file=%s records=%d", dataset_id, file.filename, record_count)

    # Fast-path duplicate check.
    existing = await get_dataset(dataset_id)
    if existing is not None:
        logger.warning("[UPLOAD][CONFLICT] dataset=%s already exists", dataset_id)
        raise HTTPException(
            status_code=409,
            detail=f"Dataset '{dataset_id}' already exists",
        )

    # Upload raw payload to object storage.
    ensure_bucket()
    put_json(dataset_id, KEY_RAW, data)

    # Persist Dataset entity.
    entity = DatasetEntity(
        dataset_id=dataset_id,
        filename=file.filename or "unknown.json",
        celery_task_id="",  # populated below once the chain is dispatched
        status=DatasetStatus.QUEUED,
    )
    try:
        await create_dataset(entity)
    except DuplicateKeyError:
        logger.warning("[UPLOAD][RACE] dataset=%s lost insert race", dataset_id)
        raise HTTPException(
            status_code=409,
            detail=f"Dataset '{dataset_id}' already exists",
        )

    # Dispatch processing workflow — only the dataset_id travels in the
    # Celery messages; each stage pulls/pushes its payload from storage.
    result = build_processing_workflow(dataset_id)
    if result is not None and getattr(result, "id", None):
        await set_celery_task_id(dataset_id, result.id)
    logger.info("[UPLOAD][DISPATCHED] dataset=%s task=%s", dataset_id, getattr(result, "id", None))

    return DatasetUploadResponse(
        dataset_id=dataset_id,
        status=DatasetStatus.QUEUED.value,
    )


@router.get("/datasets", response_model=list[DatasetStatusResponse])
async def get_all_datasets():
    """List all datasets with their current processing status."""
    docs = await list_datasets()
    return docs


@router.get("/dataset/{dataset_id}", response_model=DatasetStatusResponse)
async def get_dataset_status(dataset_id: str):
    """Get the status and result of a specific dataset."""
    doc = await get_dataset(dataset_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return doc


@router.delete("/dataset/{dataset_id}")
async def delete_dataset_endpoint(dataset_id: str):
    """Delete a dataset. Only allowed when status is COMPLETED or FAILED."""
    doc = await get_dataset(dataset_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    if doc["status"] not in TERMINAL_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete dataset '{dataset_id}' while it is still processing (status: {doc['status']})",
        )
    try:
        delete_prefix(dataset_id)
    except Exception as e:
        logger.error("[DELETE][S3_FAIL] dataset=%s error=%s", dataset_id, e)
        raise HTTPException(status_code=500, detail=f"Failed to delete object store payload: {e}")
    await delete_dataset(dataset_id)
    logger.info("[DELETE] dataset=%s", dataset_id)
    return {"message": f"Dataset '{dataset_id}' deleted"}


@router.delete("/datasets")
async def delete_all_datasets_endpoint():
    """Delete all datasets that are COMPLETED or FAILED.

    Ordering: identify terminal ids → purge S3 → delete Mongo rows for the
    ids whose S3 cleanup succeeded.
    """
    ids = await list_terminal_dataset_ids()
    deletable: list[str] = []
    failures: list[str] = []
    for dataset_id in ids:
        try:
            delete_prefix(dataset_id)
            deletable.append(dataset_id)
        except Exception as e:
            failures.append(dataset_id)
            logger.error("[DELETE_ALL][S3_FAIL] dataset=%s error=%s", dataset_id, e)

    removed = await delete_datasets_by_ids(deletable)
    logger.info("[DELETE_ALL] removed=%d s3_failures=%d", removed, len(failures))
    return {"message": f"{removed} dataset(s) deleted"}
