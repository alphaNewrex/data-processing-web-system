from enum import Enum
from datetime import datetime, timezone


class DatasetStatus(str, Enum):
    QUEUED = "QUEUED"
    PREPROCESSING = "PREPROCESSING"
    COMPUTING = "COMPUTING"
    SUMMARISING = "SUMMARISING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# Convenience set so callers don't hard-code string literals.
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {DatasetStatus.COMPLETED.value, DatasetStatus.FAILED.value}
)


def _parse_dt(value: object) -> datetime:
    """Coerce Mongo-stored timestamps back into aware datetimes.

    Accepts ISO-8601 strings (persisted rows), datetime instances (mongomock
    returns these directly), or None (missing field) and falls back to now(UTC)
    so the entity is always usable.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class DatasetEntity:
    """Dataset document stored in MongoDB."""

    def __init__(
        self,
        dataset_id: str,
        filename: str,
        celery_task_id: str,
        status: DatasetStatus = DatasetStatus.QUEUED,
        result: dict | None = None,
        error: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
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
            celery_task_id=data.get("celery_task_id", ""),
            status=DatasetStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            created_at=_parse_dt(data.get("created_at")),
            updated_at=_parse_dt(data.get("updated_at")),
        )
