from celery import Celery
from celery.signals import worker_process_init

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
    # Time limits per task (seconds). soft_time_limit raises SoftTimeLimitExceeded
    # which the task can catch for graceful cleanup; time_limit hard-kills the worker.
    task_soft_time_limit=60,
    task_time_limit=120,
)


@worker_process_init.connect
def _reset_clients_after_fork(**_kwargs) -> None:
    """Drop cached PyMongo / boto3 clients inherited from the master process.

    Celery's prefork pool forks the master after module import, so any
    connection pool or SSL context created pre-fork would be shared across
    children — PyMongo explicitly warns against this and it manifests as
    hangs or corrupted responses under load. Clearing the module-level
    caches forces each child to lazy-init its own clients.
    """
    # Local imports to keep module import order loose.
    from common import store as store_mod
    from common import storage as storage_mod

    store_mod.reset_client()
    storage_mod._client = None
