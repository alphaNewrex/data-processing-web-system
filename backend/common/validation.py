"""
Pure-function validation helpers for dataset records.

Kept independent of Celery and the storage layer so it can be reused
(e.g. for eager API-side validation) and tested in isolation.
"""

from datetime import datetime


REQUIRED_FIELDS = ("id", "timestamp", "value", "category")


def is_valid_record(record: object) -> bool:
    """
    Return True if `record` conforms to the expected schema:
      - dict with keys: id, timestamp, value, category (all non-null)
      - `timestamp` is an ISO-8601 parseable string (Z or offset accepted)
      - `value` is numeric (int or float, but not bool)
      - `category` is a non-empty string
    """
    if not isinstance(record, dict):
        return False

    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            return False

    timestamp = record["timestamp"]
    if not isinstance(timestamp, str):
        return False
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False

    value = record["value"]
    # Reject bool explicitly — bool is a subclass of int in Python.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False

    category = record["category"]
    if not isinstance(category, str) or not category.strip():
        return False

    return True
