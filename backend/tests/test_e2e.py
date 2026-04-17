"""
End-to-end tests for the Data Processing API.

These tests hit the real FastAPI app with a test MongoDB and mock Celery.
Run with: PYTHONPATH=. pytest tests/ -v
"""

import json
import io
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.main import app
from common.models import DatasetEntity, DatasetStatus


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
    """Test the record validation logic directly."""

    def test_valid_record(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"}
        assert _validate_record(record) is True

    def test_missing_id(self):
        from workers.tasks import _validate_record
        record = {"timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"}
        assert _validate_record(record) is False

    def test_missing_timestamp(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "value": 10, "category": "A"}
        assert _validate_record(record) is False

    def test_missing_value(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "category": "A"}
        assert _validate_record(record) is False

    def test_missing_category(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10}
        assert _validate_record(record) is False

    def test_invalid_timestamp(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "not-a-date", "value": 10, "category": "A"}
        assert _validate_record(record) is False

    def test_non_numeric_value(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": "abc", "category": "A"}
        assert _validate_record(record) is False

    def test_empty_category(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": ""}
        assert _validate_record(record) is False

    def test_null_fields(self):
        from workers.tasks import _validate_record
        record = {"id": "r1", "timestamp": None, "value": 10, "category": "A"}
        assert _validate_record(record) is False

    def test_not_a_dict(self):
        from workers.tasks import _validate_record
        assert _validate_record("not a dict") is False
        assert _validate_record(None) is False


# -------------------------------------------------------------------
# Task pipeline tests (unit, no Celery broker needed)
# -------------------------------------------------------------------

class TestTaskPipeline:
    """Test the task functions directly (without Celery broker)."""

    @patch("workers.tasks.update_dataset_status")
    def test_preprocess(self, mock_update, sample_dataset):
        from workers.tasks import preprocess
        result = preprocess("test_ds_001", sample_dataset)

        assert result["dataset_id"] == "test_ds_001"
        assert result["record_count"] == 6
        assert result["invalid_count"] == 2
        assert len(result["valid_records"]) == 4
        mock_update.assert_called_once_with("test_ds_001", DatasetStatus.PREPROCESSING)

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_compute(self, mock_sleep, mock_update):
        from workers.tasks import compute
        prev = {
            "dataset_id": "test_ds_001",
            "record_count": 6,
            "invalid_count": 2,
            "valid_records": [
                {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10, "category": "A"},
                {"id": "r2", "timestamp": "2026-01-01T10:01:00Z", "value": 20, "category": "B"},
                {"id": "r3", "timestamp": "2026-01-01T10:02:00Z", "value": 30, "category": "A"},
                {"id": "r4", "timestamp": "2026-01-01T10:03:00Z", "value": 15, "category": "C"},
            ],
        }
        result = compute(prev)

        assert result["category_summary"] == {"A": 2, "B": 1, "C": 1}
        assert result["average_value"] == 18.75
        assert result["record_count"] == 6
        assert result["invalid_count"] == 2
        mock_sleep.assert_called_once_with(15)
        mock_update.assert_called_once_with("test_ds_001", DatasetStatus.COMPUTING)

    @patch("workers.tasks.set_dataset_result")
    @patch("workers.tasks.update_dataset_status")
    def test_summarise(self, mock_update, mock_set_result):
        from workers.tasks import summarise
        prev = {
            "dataset_id": "test_ds_001",
            "record_count": 6,
            "invalid_count": 2,
            "category_summary": {"A": 2, "B": 1, "C": 1},
            "average_value": 18.75,
        }
        result = summarise(prev)

        assert result["dataset_id"] == "test_ds_001"
        assert result["record_count"] == 6
        assert result["category_summary"] == {"A": 2, "B": 1, "C": 1}
        assert result["average_value"] == 18.75
        assert result["invalid_records"] == 2
        mock_set_result.assert_called_once()

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.set_dataset_result")
    @patch("workers.tasks.time.sleep")
    def test_full_pipeline(self, mock_sleep, mock_set_result, mock_update, sample_dataset):
        """Run preprocess -> compute -> summarise as a chain."""
        from workers.tasks import preprocess, compute, summarise

        r1 = preprocess("test_ds_001", sample_dataset)
        r2 = compute(r1)
        r3 = summarise(r2)

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

    @patch("api.routes.on_pipeline_error")
    @patch("api.routes.summarise")
    @patch("api.routes.compute")
    @patch("api.routes.preprocess")
    @patch("api.routes.create_dataset")
    @patch("api.routes.get_dataset")
    def test_upload_success(self, mock_get, mock_create, mock_preprocess, mock_compute, mock_summarise, mock_error, client, sample_dataset):
        mock_get.return_value = None  # no duplicate
        mock_create.return_value = None

        # Mock the chain
        mock_chain_result = MagicMock()
        mock_chain_result.id = "test-task-id"
        mock_preprocess.s.return_value.chain = MagicMock(return_value=mock_chain_result)

        with patch("api.routes.chain") as mock_chain_fn:
            mock_chain_fn.return_value.apply_async.return_value = mock_chain_result

            response = client.post("/api/dataset", files=[_make_file(sample_dataset, "ds_001.json")])

        assert response.status_code == 200
        data = response.json()
        assert data["dataset_id"] == "test_ds_001"
        assert data["status"] == "QUEUED"

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
    def test_empty_records(self, mock_update):
        from workers.tasks import preprocess
        data = {"dataset_id": "empty", "records": []}
        result = preprocess("empty", data)
        assert result["record_count"] == 0
        assert result["invalid_count"] == 0
        assert result["valid_records"] == []

    @patch("workers.tasks.update_dataset_status")
    @patch("workers.tasks.time.sleep")
    def test_compute_no_valid_records(self, mock_sleep, mock_update):
        from workers.tasks import compute
        prev = {"dataset_id": "empty", "record_count": 0, "invalid_count": 0, "valid_records": []}
        result = compute(prev)
        assert result["average_value"] == 0.0
        assert result["category_summary"] == {}

    @patch("workers.tasks.update_dataset_status")
    def test_all_invalid_records(self, mock_update):
        from workers.tasks import preprocess
        data = {
            "dataset_id": "all_invalid",
            "records": [
                {"id": "r1"},  # missing timestamp, value, category
                {"timestamp": "2026-01-01T10:00:00Z"},  # missing id, value, category
                {},  # empty
            ],
        }
        result = preprocess("all_invalid", data)
        assert result["record_count"] == 3
        assert result["invalid_count"] == 3
        assert result["valid_records"] == []

    @patch("workers.tasks.update_dataset_status")
    def test_float_values(self, mock_update):
        from workers.tasks import preprocess
        data = {
            "dataset_id": "floats",
            "records": [
                {"id": "r1", "timestamp": "2026-01-01T10:00:00Z", "value": 10.5, "category": "A"},
                {"id": "r2", "timestamp": "2026-01-01T10:01:00Z", "value": 20.3, "category": "A"},
            ],
        }
        result = preprocess("floats", data)
        assert result["invalid_count"] == 0
        assert len(result["valid_records"]) == 2
