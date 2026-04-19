"""
Dataset processing workflow definition.

Defines the Celery chain: preprocess → compute → summarise.
Only the dataset_id travels through the Celery broker; every stage
pulls its input (and pushes its output) from object storage.
"""

from celery import chain

from workers.tasks import preprocess, compute, summarise


def build_processing_workflow(dataset_id: str):
    """
    Dispatch the dataset processing workflow to Celery.

    The workflow is a Celery chain of 3 stages:
      1. Preprocess — read raw.json, validate, write preprocessed.json
      2. Compute   — read preprocessed.json, compute summary, write computed.json
      3. Summarise — read computed.json, write result.json, persist summary

    Failure handling is centralised in the `DatasetTask` base class: if any
    stage exhausts its retries, `DatasetTask.on_failure` marks the dataset
    as FAILED in MongoDB. No per-workflow error callback is needed.

    Returns the AsyncResult for the chain. Callers may ignore it (progress is
    tracked via the Dataset entity in MongoDB, not the Celery result backend).
    """
    pipeline = chain(
        preprocess.s(dataset_id),
        compute.s(),
        summarise.s(),
    )
    return pipeline.apply_async()

