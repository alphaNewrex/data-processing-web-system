from pydantic import BaseModel


class DatasetUploadResponse(BaseModel):
    dataset_id: str
    status: str


class DatasetStatusResponse(BaseModel):
    dataset_id: str
    filename: str
    status: str
    result: dict | None = None
    error: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ErrorResponse(BaseModel):
    detail: str
