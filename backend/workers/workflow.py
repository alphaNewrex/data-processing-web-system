"""
Dataset processing workflow definition.

Defines the Celery chain: preprocess → compute → summarise
with error handling callback.
"""

from celery import chain

from workers.tasks import preprocess, compute, summarise, on_pipeline_error


def build_processing_workflow(dataset_id: str, data: dict):
    """
    Build and dispatch the dataset processing workflow.

    The workflow is a Celery chain of 3 stages:
      1. Preprocess — validate records, separate valid/invalid
      2. Compute   — category summary, average value (with simulated delay)
      3. Summarise — assemble final output, persist to MongoDB

    If any stage fails, on_pipeline_error marks the dataset as FAILED.

    Returns the AsyncResult for the chain.
    """
    pipeline = chain(
        preprocess.s(dataset_id, data),
        compute.s(),
        summarise.s(),
    )

    return pipeline.apply_async(
        link_error=on_pipeline_error.s(dataset_id=dataset_id),
    )
