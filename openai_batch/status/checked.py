import logging
from typing import Iterable

import openai

from .. import runner
from ..db import schema
from ..model import BatchStatus

logger = logging.getLogger(__name__)


def check(batch_ids: Iterable[str]) -> Iterable[BatchStatus]:
    cursor = openai.batches.list(limit=100)
    statuses: list[BatchStatus] = []
    batch_ids = set(batch_ids)

    for batch in cursor:
        if batch.id in batch_ids:
            statuses.append(BatchStatus.from_batch(batch))
            batch_ids.remove(batch.id)

        if len(batch_ids) == 0:
            break

    found_ids = {
        batch_id  #
        for status in statuses
        if (batch_id := status.batch_id) is not None
    }
    not_found_ids = batch_ids - found_ids

    for batch_id in not_found_ids:
        statuses.append(BatchStatus(message=f"batch with id {batch_id} not found"))

    return statuses


def to_checked(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    raise NotImplementedError
