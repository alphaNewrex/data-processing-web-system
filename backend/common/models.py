from enum import Enum
from datetime import datetime, timezone
from typing import Optional


class DatasetStatus(str, Enum):
    QUEUED = "QUEUED"
    PREPROCESSING = "PREPROCESSING"
    COMPUTING = "COMPUTING"
    SUMMARISING = "SUMMARISING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class DatasetEntity:
    """Dataset document stored in MongoDB."""

    def __init__(
        self,
        dataset_id: str,
        filename: str,
        celery_task_id: str,
        status: DatasetStatus = DatasetStatus.QUEUED,
        result: Optional[dict] = None,
        error: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.dataset_id = dataset_id
        self.filename = filename
        self.celery_task_id = celery_task_id
        self.status = status
        self.result = result
        self.error = error
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "filename": self.filename,
            "celery_task_id": self.celery_task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DatasetEntity":
        return cls(
            dataset_id=data["dataset_id"],
            filename=data["filename"],
            celery_task_id=data["celery_task_id"],
            status=DatasetStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at"),
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else data.get("updated_at"),
        )
