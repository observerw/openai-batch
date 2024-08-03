import itertools
import logging
import os
from dataclasses import dataclass
from typing import Iterable

import openai

from .. import runner
from ..db import schema
from ..db.database import works_db
from ..model import BatchStatus
from .exception import StatusInterrupt
from .utils import download, download_error

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    statuses: list[BatchStatus]
    not_found_ids: set[str]


def check(batch_ids: Iterable[str]) -> CheckResult:
    cursor = openai.batches.list(limit=100)
    statuses: list[BatchStatus] = []
    batch_ids = set(batch_ids)

    for batch in cursor:
        if batch.id in batch_ids:
            statuses.append(BatchStatus(batch=batch))
            batch_ids.remove(batch.id)

        if len(batch_ids) == 0:
            break

    found_ids = {
        batch_id  #
        for status in statuses
        if (batch_id := status.batch_id) is not None
    }
    not_found_ids = batch_ids - found_ids

    return CheckResult(
        statuses=statuses,
        not_found_ids=not_found_ids,
    )


def to_checked(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    result = check(work.undone_batch_ids)
    if not_found_ids := result.not_found_ids:
        logger.warning(f"Batch IDs not found: {not_found_ids}")

    statuses = sorted(result.statuses, key=lambda status: status.status)

    # download success and failed batch files
    grouped = itertools.groupby(
        statuses,
        key=lambda status: status.status,
    )
    for status, statuses in grouped:
        ids = [id for status in statuses if (id := status.batch_id)]
        match status:
            case "success":
                download(cls, ids)
            case "failed":
                download_error(cls, ids)
                logger.warning(f"Batch failed: {ids}")

    # update work in database
    assert work.id
    with works_db.update_work(work.id) as work:
        done_batch_ids = {
            status.batch_id  #
            for status in statuses
            if status.status != "in_progress"
        }

        # exclude done_batch_ids from undone_batch_ids
        work.undone_batch_ids = list(set(work.undone_batch_ids) - done_batch_ids)
        # add done_batch_ids to done_batch_ids
        work.done_batch_ids = list(set(work.done_batch_ids) | done_batch_ids)

    works_db.update_process_status(
        pid=os.getpid(),
        description="Checked",
        status=None,
    )

    # all batches are done, marked as completed
    if not work.undone_batch_ids:
        raise StatusInterrupt(schema.WorkStatus.Completed)

    return work
