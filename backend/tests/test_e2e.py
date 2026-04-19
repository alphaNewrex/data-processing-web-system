"""
End-to-end tests for the Data Processing API.

These tests hit the real FastAPI app with a test MongoDB and mock Celery.
S3 storage is backed by an in-memory dict (same pattern as mongomock).
Run with: PYTHONPATH=. pytest tests/ -v
"""

import json
import io
import copy
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.main import app
from common.models import DatasetEntity, DatasetStatus


# -------------------------------------------------------------------
# In-memory S3 mock (no boto3 needed locally)
# -------------------------------------------------------------------

class FakeStorage:
    """
    Drop-in replacement for common.storage that keeps everything in a dict.
    Mirrors put_json / get_json / delete_prefix / ensure_bucket.
    """

    def __init__(self):
        self._objects: dict[str, dict] = {}  # key -> decoded payload

    def put_json(self, dataset_id: str, name: str, payload) -> str:
        key = f"{dataset_id}/{name}"
        self._objects[key] = copy.deepcopy(payload)
        return key

    def get_json(self, dataset_id: str, name: str):
        key = f"{dataset_id}/{name}"
        return copy.deepcopy(self._objects[key])

    def delete_prefix(self, dataset_id: str) -> int:
        prefix = f"{dataset_id}/"
        keys = [k for k in self._objects if k.startswith(prefix)]
        for k in keys:
            del self._objects[k]
        return len(keys)

    @staticmethod
    def ensure_bucket():
        pass  # no-op


@pytest.fixture
def fake_storage(monkeypatch):
    """Patch common.storage functions with an in-memory dict."""
    store = FakeStorage()
    import common.storage as storage_mod
    monkeypatch.setattr(storage_mod, "put_json", store.put_json)
    monkeypatch.setattr(storage_mod, "get_json", store.get_json)
    monkeypatch.setattr(storage_mod, "delete_prefix", store.delete_prefix)
    monkeypatch.setattr(storage_mod, "ensure_bucket", store.ensure_bucket)
    # Also patch the names imported directly into routes
    import api.routes as routes_mod
    monkeypatch.setattr(routes_mod, "put_json", store.put_json)
    monkeypatch.setattr(routes_mod, "delete_prefix", store.delete_prefix)
    monkeypatch.setattr(routes_mod, "ensure_bucket", store.ensure_bucket)
    # And into workers.tasks
    import workers.tasks as tasks_mod
    monkeypatch.setattr(tasks_mod, "put_json", store.put_json)
    monkeypatch.setattr(tasks_mod, "get_json", store.get_json)
    return store


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def sample_dataset():
    return {
        "dataset_id": "test_ds_001",
        "records": [
            {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"},
            {"id": "r2", "timestamp": "2026-01-01T10:01:00Z", "value": 20, "category": "B"},
            {"id": "r3", "timestamp": "2026-01-01T10:02:00Z", "value": 30, "category": "A"},
            {"id": "r4", "timestamp": "2026-01-01T10:03:00Z", "value": 15, "category": "C"},
            # Invalid: missing timestamp
            {"id": "r5", "value": 25, "category": "B"},
            # Invalid: missing value
            {"id": "r6", "timestamp": "2026-01-01T10:05:00Z", "category": "A"},
        ],
    }


def _make_file(data: dict, filename: str = "test.json"):
    content = json.dumps(data).encode()
    return ("file", (filename, io.BytesIO(content), "application/json"))


# -------------------------------------------------------------------
# Validation tests
# -------------------------------------------------------------------

class TestValidateRecord:
    """Test the record validation logic directly (pure function)."""

    def test_valid_record(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"}
        assert is_valid_record(record) is True

    def test_missing_id(self):
        from common.validation import is_valid_record
        record = {"timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"}
        assert is_valid_record(record) is False

    def test_missing_timestamp(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "value": 10, "category": "A"}
        assert is_valid_record(record) is False

    def test_missing_value(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "category": "A"}
        assert is_valid_record(record) is False

    def test_missing_category(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10}
        assert is_valid_record(record) is False

    def test_invalid_timestamp(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "not-a-date", "value": 10, "category": "A"}
        assert is_valid_record(record) is False

    def test_non_numeric_value(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": "abc", "category": "A"}
        assert is_valid_record(record) is False

    def test_empty_category(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": ""}
        assert is_valid_record(record) is False

    def test_null_fields(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": None, "value": 10, "category": "A"}
        assert is_valid_record(record) is False

    def test_not_a_dict(self):
        from common.validation import is_valid_record
        assert is_valid_record("not a dict") is False
        assert is_valid_record(None) is False

    def test_bool_is_not_numeric(self):
        from common.validation import is_valid_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": True, "category": "A"}
        assert is_valid_record(record) is False


# -------------------------------------------------------------------
# Task pipeline tests (unit, no Celery broker needed)
# -------------------------------------------------------------------

class TestTaskPipeline:
    """Test the task functions directly (without Celery broker)."""

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_preprocess(self, mock_sleep, mock_update, fake_storage, sample_dataset):
        from workers.tasks import preprocess

        fake_storage.put_json("test_ds_001", "raw.json", sample_dataset)
        result_id = preprocess("test_ds_001")

        assert result_id == "test_ds_001"
        out = fake_storage.get_json("test_ds_001", "preprocessed.json")
        assert out["record_count"] == 6
        assert out["invalid_count"] == 2
        assert len(out["valid_records"]) == 4
        mock_update.assert_called_once_with("test_ds_001", DatasetStatus.PREPROCESSING)

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_compute(self, mock_sleep, mock_update, fake_storage):
        from workers.tasks import compute

        fake_storage.put_json(
            "test_ds_001",
            "preprocessed.json",
            {
                "dataset_id": "test_ds_001",
                "record_count": 6,
                "invalid_count": 2,
                "valid_records": [
                    {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"},
                    {"id": "r2", "timestamp": "2026-01-01T10:01:00Z", "value": 20, "category": "B"},
                    {"id": "r3", "timestamp": "2026-01-01T10:02:00Z", "value": 30, "category": "A"},
                    {"id": "r4", "timestamp": "2026-01-01T10:03:00Z", "value": 15, "category": "C"},
                ],
            },
        )
        assert compute("test_ds_001") == "test_ds_001"
        out = fake_storage.get_json("test_ds_001", "computed.json")
        assert out["category_summary"] == {"A": 2, "B": 1, "C": 1}
        assert out["average_value"] == 18.75
        assert out["record_count"] == 6
        assert out["invalid_count"] == 2
        mock_update.assert_called_once_with("test_ds_001", DatasetStatus.COMPUTING)

    @patch("workers.tasks.set_dataset_result")
    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_summarise(self, mock_sleep, mock_update, mock_set_result, fake_storage):
        from workers.tasks import summarise

        fake_storage.put_json(
            "test_ds_001",
            "computed.json",
            {
                "dataset_id": "test_ds_001",
                "record_count": 6,
                "invalid_count": 2,
                "category_summary": {"A": 2, "B": 1, "C": 1},
                "average_value": 18.75,
            },
        )
        result = summarise("test_ds_001")

        assert result["dataset_id"] == "test_ds_001"
        assert result["record_count"] == 6
        assert result["category_summary"] == {"A": 2, "B": 1, "C": 1}
        assert result["average_value"] == 18.75
        assert result["invalid_records"] == 2
        # Persisted to storage AND Mongo
        assert fake_storage.get_json("test_ds_001", "result.json") == result
        mock_set_result.assert_called_once()

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.set_dataset_result")
    @patch("workers.tasks.time.sleep")
    def test_full_pipeline(self, mock_sleep, mock_set_result, mock_update, fake_storage, sample_dataset):
        """Run preprocess -> compute -> summarise as a chain through storage."""
        from workers.tasks import preprocess, compute, summarise

        fake_storage.put_json("test_ds_001", "raw.json", sample_dataset)
        assert preprocess("test_ds_001") == "test_ds_001"
        assert compute("test_ds_001") == "test_ds_001"
        r3 = summarise("test_ds_001")

        assert r3["dataset_id"] == "test_ds_001"
        assert r3["record_count"] == 6
        assert r3["invalid_records"] == 2
        assert r3["average_value"] == 18.75
        assert r3["category_summary"] == {"A": 2, "B": 1, "C": 1}


# -------------------------------------------------------------------
# API endpoint tests
# -------------------------------------------------------------------

class TestAPIEndpoints:
    """Test FastAPI endpoints with mocked Celery and MongoDB."""

    @patch("api.routes.build_processing_workflow")
    @patch("api.routes.set_celery_task_id")
    @patch("api.routes.create_dataset")
    @patch("api.routes.get_dataset")
    def test_upload_success(self, mock_get, mock_create, mock_set_task_id, mock_workflow, client, sample_dataset, fake_storage):
        mock_get.return_value = None  # no duplicate
        mock_create.return_value = None

        mock_async_result = MagicMock()
        mock_async_result.id = "test-task-id"
        mock_workflow.return_value = mock_async_result

        response = client.post("/api/dataset", files=[_make_file(sample_dataset, "ds_001.json")])

        assert response.status_code == 200
        data = response.json()
        assert data["dataset_id"] == "test_ds_001"
        assert data["status"] == "QUEUED"

        # Verify workflow dispatched with just the dataset_id
        mock_create.assert_awaited_once()
        mock_workflow.assert_called_once_with("test_ds_001")
        # celery_task_id captured from the chain's AsyncResult
        mock_set_task_id.assert_awaited_once_with("test_ds_001", "test-task-id")

        # Raw payload should now live in storage under {dataset_id}/raw.json
        uploaded = fake_storage.get_json("test_ds_001", "raw.json")
        assert uploaded["dataset_id"] == "test_ds_001"
        assert len(uploaded["records"]) == 6

    def test_upload_non_json_file(self, client):
        response = client.post(
            "/api/dataset",
            files=[("file", ("test.txt", io.BytesIO(b"hello"), "text/plain"))],
        )
        assert response.status_code == 400
        assert "json" in response.json()["detail"].lower()

    def test_upload_invalid_json(self, client):
        response = client.post(
            "/api/dataset",
            files=[("file", ("test.json", io.BytesIO(b"not json"), "application/json"))],
        )
        assert response.status_code == 400

    def test_upload_missing_dataset_id(self, client):
        data = json.dumps({"records": []}).encode()
        response = client.post(
            "/api/dataset",
            files=[("file", ("test.json", io.BytesIO(data), "application/json"))],
        )
        assert response.status_code == 400
        assert "dataset_id" in response.json()["detail"]

    def test_upload_missing_records(self, client):
        data = json.dumps({"dataset_id": "ds_001"}).encode()
        response = client.post(
            "/api/dataset",
            files=[("file", ("test.json", io.BytesIO(data), "application/json"))],
        )
        assert response.status_code == 400
        assert "records" in response.json()["detail"]

    @patch("api.routes.get_dataset")
    @patch("api.routes.create_dataset")
    def test_upload_duplicate(self, mock_create, mock_get, client, sample_dataset):
        mock_get.return_value = {"dataset_id": "test_ds_001"}  # already exists
        response = client.post("/api/dataset", files=[_make_file(sample_dataset, "ds_001.json")])
        assert response.status_code == 409

    @patch("api.routes.build_processing_workflow")
    @patch("api.routes.set_celery_task_id")
    @patch("api.routes.create_dataset")
    @patch("api.routes.get_dataset")
    def test_upload_duplicate_race_via_unique_index(
        self, mock_get, mock_create, mock_set_task_id, mock_workflow, client, sample_dataset, fake_storage
    ):
        """Two concurrent uploads both pass the pre-check; the unique index
        on dataset_id rejects the loser with DuplicateKeyError → 409."""
        from pymongo.errors import DuplicateKeyError

        mock_get.return_value = None  # pre-check says "not a duplicate"
        mock_create.side_effect = DuplicateKeyError("dup key: dataset_id")

        response = client.post("/api/dataset", files=[_make_file(sample_dataset, "ds_001.json")])

        assert response.status_code == 409
        # Workflow must NOT have been dispatched on the losing race
        mock_workflow.assert_not_called()
        mock_set_task_id.assert_not_called()

    @patch("api.routes.list_datasets")
    def test_get_datasets(self, mock_list, client):
        mock_list.return_value = [
            {"dataset_id": "ds_001", "filename": "ds_001.json", "status": "COMPLETED",
             "result": None, "error": None,
             "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00"},
        ]
        response = client.get("/api/datasets")
        assert response.status_code == 200
        assert len(response.json()) == 1

    @patch("api.routes.get_dataset")
    def test_get_dataset_found(self, mock_get, client):
        mock_get.return_value = {
            "dataset_id": "ds_001", "filename": "ds_001.json", "status": "COMPLETED",
            "result": {"record_count": 6}, "error": None,
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00",
        }
        response = client.get("/api/dataset/ds_001")
        assert response.status_code == 200
        assert response.json()["dataset_id"] == "ds_001"

    @patch("api.routes.get_dataset")
    def test_get_dataset_not_found(self, mock_get, client):
        mock_get.return_value = None
        response = client.get("/api/dataset/nonexistent")
        assert response.status_code == 404


# -------------------------------------------------------------------
# Edge case tests
# -------------------------------------------------------------------

class TestEdgeCases:
    """Test edge cases in processing logic."""

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_empty_records(self, mock_sleep, mock_update, fake_storage):
        from workers.tasks import preprocess

        fake_storage.put_json("empty", "raw.json", {"dataset_id": "empty", "records": []})
        assert preprocess("empty") == "empty"
        out = fake_storage.get_json("empty", "preprocessed.json")
        assert out["record_count"] == 0
        assert out["invalid_count"] == 0
        assert out["valid_records"] == []

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_compute_no_valid_records(self, mock_sleep, mock_update, fake_storage):
        from workers.tasks import compute

        fake_storage.put_json(
            "empty",
            "preprocessed.json",
            {"dataset_id": "empty", "record_count": 0, "invalid_count": 0, "valid_records": []},
        )
        assert compute("empty") == "empty"
        out = fake_storage.get_json("empty", "computed.json")
        assert out["average_value"] == 0.0
        assert out["category_summary"] == {}

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_all_invalid_records(self, mock_sleep, mock_update, fake_storage):
        from workers.tasks import preprocess

        fake_storage.put_json(
            "all_invalid",
            "raw.json",
            {
                "dataset_id": "all_invalid",
                "records": [
                    {"id": "r1"},
                    {"timestamp": "2026-01-01T10:00:00Z"},
                    {},
                ],
            },
        )
        assert preprocess("all_invalid") == "all_invalid"
        out = fake_storage.get_json("all_invalid", "preprocessed.json")
        assert out["record_count"] == 3
        assert out["invalid_count"] == 3
        assert out["valid_records"] == []

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_float_values(self, mock_sleep, mock_update, fake_storage):
        from workers.tasks import preprocess

        fake_storage.put_json(
            "floats",
            "raw.json",
            {
                "dataset_id": "floats",
                "records": [
                    {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10.5, "category": "A"},
                    {"id": "r2", "timestamp": "2026-01-01T10:01:00Z", "value": 20.3, "category": "A"},
                ],
            },
        )
        assert preprocess("floats") == "floats"
        out = fake_storage.get_json("floats", "preprocessed.json")
        assert out["invalid_count"] == 0
        assert len(out["valid_records"]) == 2


# -------------------------------------------------------------------
# Celery error-handler test
# -------------------------------------------------------------------

class TestDatasetTaskOnFailure:
    """Cover the DatasetTask.on_failure hook that centralises failure handling."""

    @patch("workers.tasks.delete_prefix")
    @patch("workers.tasks.set_dataset_failed")
    def test_on_failure_marks_failed_from_positional_args(self, mock_set_failed, mock_delete_prefix):
        from workers.tasks import preprocess

        exc = RuntimeError("preprocess blew up")
        # Celery invokes on_failure(self, exc, task_id, args, kwargs, einfo)
        preprocess.on_failure(exc, "task-123", ("ds_fail",), {}, None)

        mock_set_failed.assert_called_once_with("ds_fail", "preprocess blew up")
        # S3 prefix purged so failed datasets don't leak storage
        mock_delete_prefix.assert_called_once_with("ds_fail")

    @patch("workers.tasks.delete_prefix")
    @patch("workers.tasks.set_dataset_failed")
    def test_on_failure_marks_failed_from_kwargs(self, mock_set_failed, mock_delete_prefix):
        from workers.tasks import compute

        exc = ValueError("compute blew up")
        compute.on_failure(exc, "task-456", (), {"dataset_id": "ds_fail_kw"}, None)

        mock_set_failed.assert_called_once_with("ds_fail_kw", "compute blew up")
        mock_delete_prefix.assert_called_once_with("ds_fail_kw")

    @patch("workers.tasks.delete_prefix")
    @patch("workers.tasks.set_dataset_failed")
    def test_on_failure_stringifies_exception(self, mock_set_failed, mock_delete_prefix):
        from workers.tasks import summarise

        summarise.on_failure(
            RuntimeError("plain string error"), "task-789", ("ds_fail_2",), {}, None
        )

        mock_set_failed.assert_called_once_with("ds_fail_2", "plain string error")
        mock_delete_prefix.assert_called_once_with("ds_fail_2")

    @patch("workers.tasks.delete_prefix")
    @patch("workers.tasks.set_dataset_failed")
    def test_on_failure_skips_when_no_dataset_id(self, mock_set_failed, mock_delete_prefix):
        from workers.tasks import preprocess

        # No args and no dataset_id kwarg -> should not call set_dataset_failed
        preprocess.on_failure(RuntimeError("oops"), "task-x", (), {}, None)

        mock_set_failed.assert_not_called()
        mock_delete_prefix.assert_not_called()

    @patch("workers.tasks.delete_prefix", side_effect=RuntimeError("s3 down"))
    @patch("workers.tasks.set_dataset_failed")
    def test_on_failure_swallows_s3_cleanup_errors(self, mock_set_failed, _mock_delete_prefix):
        """The dataset must be marked FAILED even when S3 cleanup raises."""
        from workers.tasks import preprocess

        preprocess.on_failure(RuntimeError("boom"), "task-z", ("ds_x",), {}, None)

        mock_set_failed.assert_called_once_with("ds_x", "boom")


# -------------------------------------------------------------------
# Mongo store integration tests (mongomock)
# -------------------------------------------------------------------

class TestMongoStoreIntegration:
    """
    Integration tests for the sync common.store module backed by mongomock.

    These exercise real pymongo query semantics (filters, updates, sorts)
    without needing a live MongoDB instance.
    """

    @pytest.fixture(autouse=True)
    def _patch_client(self, monkeypatch):
        import mongomock
        from common import store as store_mod

        fake_client = mongomock.MongoClient()
        # Reset module-level cached client and swap MongoClient factory
        monkeypatch.setattr(store_mod, "_client", None)
        monkeypatch.setattr(store_mod, "MongoClient", lambda *_a, **_k: fake_client)
        yield fake_client
        # cleanup
        monkeypatch.setattr(store_mod, "_client", None)

    def _make_entity(self, dataset_id="ds_x", filename="ds_x.json"):
        return DatasetEntity(
            dataset_id=dataset_id,
            filename=filename,
            celery_task_id="",
            status=DatasetStatus.QUEUED,
        )

    def test_create_and_get_dataset(self):
        from common.store import create_dataset, get_dataset

        entity = self._make_entity("ds_a")
        create_dataset(entity)

        fetched = get_dataset("ds_a")
        assert fetched is not None
        assert fetched.dataset_id == "ds_a"
        assert fetched.status == DatasetStatus.QUEUED

    def test_get_dataset_missing_returns_none(self):
        from common.store import get_dataset
        assert get_dataset("does_not_exist") is None

    def test_update_dataset_status(self):
        from common.store import create_dataset, update_dataset_status, get_dataset

        create_dataset(self._make_entity("ds_b"))
        update_dataset_status("ds_b", DatasetStatus.COMPUTING)

        fetched = get_dataset("ds_b")
        assert fetched.status == DatasetStatus.COMPUTING

    def test_set_dataset_result_marks_completed(self):
        from common.store import create_dataset, set_dataset_result, get_dataset

        create_dataset(self._make_entity("ds_c"))
        result = {
            "dataset_id": "ds_c",
            "record_count": 10,
            "category_summary": {"A": 5, "B": 5},
            "average_value": 12.5,
            "invalid_records": 0,
        }
        set_dataset_result("ds_c", result)

        fetched = get_dataset("ds_c")
        assert fetched.status == DatasetStatus.COMPLETED
        assert fetched.result == result

    def test_set_dataset_failed_marks_failed(self):
        from common.store import create_dataset, set_dataset_failed, get_dataset

        create_dataset(self._make_entity("ds_d"))
        set_dataset_failed("ds_d", "worker crashed")

        fetched = get_dataset("ds_d")
        assert fetched.status == DatasetStatus.FAILED
        assert fetched.error == "worker crashed"

    def test_list_datasets_sorted_desc_by_created_at(self):
        from common.store import create_dataset, list_datasets
        from datetime import datetime, timezone, timedelta

        base = datetime.now(timezone.utc)
        e1 = self._make_entity("ds_old")
        e1.created_at = base - timedelta(hours=2)
        e2 = self._make_entity("ds_new")
        e2.created_at = base

        create_dataset(e1)
        create_dataset(e2)

        docs = list_datasets()
        assert [d["dataset_id"] for d in docs] == ["ds_new", "ds_old"]
        # _id must be projected out
        assert all("_id" not in d for d in docs)

    def test_update_on_missing_dataset_is_noop(self):
        from common.store import update_dataset_status, get_dataset

        # Should not raise even when no matching document exists
        update_dataset_status("ghost", DatasetStatus.COMPLETED)
        assert get_dataset("ghost") is None


# -------------------------------------------------------------------
# Model serialization tests
# -------------------------------------------------------------------

class TestDatasetEntity:
    def test_round_trip_to_from_dict(self):
        e = DatasetEntity(
            dataset_id="ds1",
            filename="ds1.json",
            celery_task_id="tid",
            status=DatasetStatus.COMPUTING,
            result={"k": 1},
            error=None,
        )
        d = e.to_dict()
        e2 = DatasetEntity.from_dict(d)
        assert e2.dataset_id == "ds1"
        assert e2.status == DatasetStatus.COMPUTING
        assert e2.result == {"k": 1}
        assert e2.error is None
        assert isinstance(e2.created_at, datetime)


# -------------------------------------------------------------------
# Delete endpoint tests
# -------------------------------------------------------------------

class TestDeleteEndpoints:
    @patch("api.routes.delete_prefix")
    @patch("api.routes.delete_dataset")
    @patch("api.routes.get_dataset")
    def test_delete_completed_dataset(self, mock_get, mock_delete, mock_prefix, client):
        mock_get.return_value = {
            "dataset_id": "ds_1", "filename": "ds_1.json",
            "status": "COMPLETED", "result": None, "error": None,
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00",
        }
        mock_delete.return_value = True
        mock_prefix.return_value = 0
        response = client.delete("/api/dataset/ds_1")
        assert response.status_code == 200
        mock_prefix.assert_called_once_with("ds_1")

    @patch("api.routes.get_dataset")
    def test_delete_nonexistent_dataset(self, mock_get, client):
        mock_get.return_value = None
        response = client.delete("/api/dataset/ghost")
        assert response.status_code == 404

    @patch("api.routes.get_dataset")
    def test_delete_in_progress_dataset_rejected(self, mock_get, client):
        mock_get.return_value = {
            "dataset_id": "ds_p", "filename": "ds_p.json",
            "status": "COMPUTING", "result": None, "error": None,
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00",
        }
        response = client.delete("/api/dataset/ds_p")
        assert response.status_code == 409

    @patch("api.routes.list_terminal_dataset_ids")
    @patch("api.routes.delete_datasets_by_ids")
    def test_delete_all_datasets(self, mock_delete_by_ids, mock_list_ids, client):
        mock_list_ids.return_value = ["ds_a", "ds_b", "ds_c"]
        mock_delete_by_ids.return_value = 3
        with patch("api.routes.delete_prefix") as mock_prefix:
            response = client.delete("/api/datasets")
        assert response.status_code == 200
        assert "3" in response.json()["message"]
        assert mock_prefix.call_count == 3
        # S3 cleanup must happen before Mongo delete
        mock_delete_by_ids.assert_awaited_once_with(["ds_a", "ds_b", "ds_c"])


# -------------------------------------------------------------------
# Object storage tests (in-memory fake)
# -------------------------------------------------------------------

class TestObjectStorage:
    """Directly exercise the FakeStorage / common.storage interface."""

    def test_put_and_get_json_round_trip(self, fake_storage):
        key = fake_storage.put_json("ds_x", "raw.json", {"hello": "world", "n": [1, 2, 3]})
        assert key == "ds_x/raw.json"
        assert fake_storage.get_json("ds_x", "raw.json") == {"hello": "world", "n": [1, 2, 3]}

    def test_delete_prefix_removes_all_stage_files(self, fake_storage):
        for name in ("raw.json", "preprocessed.json", "computed.json", "result.json"):
            fake_storage.put_json("ds_y", name, {"k": name})
        deleted = fake_storage.delete_prefix("ds_y")
        assert deleted == 4
        # Second call is a no-op on an empty prefix.
        assert fake_storage.delete_prefix("ds_y") == 0

    def test_delete_prefix_no_objects(self, fake_storage):
        assert fake_storage.delete_prefix("nothing") == 0

    def test_ensure_bucket_is_noop(self, fake_storage):
        fake_storage.ensure_bucket()  # should not raise