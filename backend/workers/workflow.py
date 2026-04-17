"""
Dataset processing workflow definition.

Defines the Celery chain: preprocess → compute → summarise.
Only the dataset_id travels through the Celery broker; every stage
pulls its input (and pushes its output) from object storage.
"""

from celery import chain

from workers.tasks import preprocess, compute, summarise, on_pipeline_error


def build_processing_workflow(dataset_id: str):
    """
    Build and dispatch the dataset processing workflow.

    The workflow is a Celery chain of 3 stages:
      1. Preprocess — read raw.json, validate, write preprocessed.json
      2. Compute   — read preprocessed.json, write computed.json (15s sleep)
      3. Summarise — read computed.json, write result.json, persist summary

    If any stage fails, on_pipeline_error marks the dataset as FAILED.

    Returns the AsyncResult for the chain.
    """
    pipeline = chain(
        preprocess.s(dataset_id),
        compute.s(),
        summarise.s(),
    )

    return pipeline.apply_async(
        link_error=on_pipeline_error.s(dataset_id=dataset_id),
    )
