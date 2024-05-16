import logging
from typing import Iterable

import openai

from ..db import schema
from ..model import BatchStatus

logger = logging.getLogger(__name__)


def check(batch_ids: set[str]) -> Iterable[BatchStatus]:
    cursor = openai.batches.list(limit=100)
    statuses: list[BatchStatus] = []

    for batch in cursor:
        if batch.id in batch_ids:
            statuses.append(BatchStatus.from_batch(batch))

        if len(statuses) == len(batch_ids):
            break

    found_ids = {
        batch_id for status in statuses if (batch_id := status.batch_id) is not None
    }
    not_found_ids = batch_ids - found_ids

    for batch_id in not_found_ids:
        statuses.append(BatchStatus(message=f"batch with id {batch_id} not found"))

    return statuses


def running_stage(work: schema.Work):
    raise NotImplementedError()
