from celery import Celery

from common.config import settings

celery_app = Celery(
    "data_processing",
    broker=settings.RABBITMQ_URL,
    include=["workers.tasks"],
)

celery_app.conf.update(
    # Crash-resilient: don't ack until task completes
    task_acks_late=True,
    # Re-queue task if worker is killed mid-execution
    task_reject_on_worker_lost=True,
    # Only prefetch 1 task at a time per worker — ensures fair distribution
    worker_prefetch_multiplier=1,
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # We store results in MongoDB directly, disable Celery result backend
    result_backend=None,
    # Celery 6 forward-compat: explicitly opt into retrying broker connection at startup
    broker_connection_retry_on_startup=True,
)
