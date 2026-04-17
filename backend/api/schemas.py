from pydantic import BaseModel
from typing import Optional


class DatasetUploadResponse(BaseModel):
    dataset_id: str
    status: str


class DatasetStatusResponse(BaseModel):
    dataset_id: str
    filename: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ErrorResponse(BaseModel):
    detail: str
