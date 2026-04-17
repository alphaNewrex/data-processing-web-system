import json

from fastapi import APIRouter, UploadFile, File, HTTPException

from api.async_store import create_dataset, get_dataset, list_datasets, delete_dataset, delete_all_datasets
from api.schemas import DatasetUploadResponse, DatasetStatusResponse
from common.models import DatasetEntity, DatasetStatus
from common.storage import ensure_bucket, put_json, delete_prefix, KEY_RAW
from workers.workflow import build_processing_workflow

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

    # Check for duplicate dataset_id
    existing = await get_dataset(dataset_id)
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Dataset '{dataset_id}' already exists",
        )

    # Persist Dataset entity to MongoDB.
    entity = DatasetEntity(
        dataset_id=dataset_id,
        filename=file.filename or "unknown.json",
        celery_task_id="",  # will be set after dispatch
        status=DatasetStatus.QUEUED,
    )
    await create_dataset(entity)

    # Upload raw payload to object storage so workers can pull it by key
    # rather than receiving the full body through RabbitMQ.
    ensure_bucket()
    put_json(dataset_id, KEY_RAW, data)

    # Dispatch processing workflow — only the dataset_id travels in the
    # Celery messages; each stage pulls/pushes its payload from storage.
    build_processing_workflow(dataset_id)

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
    if doc["status"] not in (DatasetStatus.COMPLETED.value, DatasetStatus.FAILED.value):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete dataset '{dataset_id}' while it is still processing (status: {doc['status']})",
        )
    await delete_dataset(dataset_id)
    # Remove any stage payloads from object storage as well.
    delete_prefix(dataset_id)
    return {"message": f"Dataset '{dataset_id}' deleted"}


@router.delete("/datasets")
async def delete_all_datasets_endpoint():
    """Delete all datasets that are COMPLETED or FAILED."""
    ids = await delete_all_datasets()
    for dataset_id in ids:
        delete_prefix(dataset_id)
    return {"message": f"{len(ids)} dataset(s) deleted"}
