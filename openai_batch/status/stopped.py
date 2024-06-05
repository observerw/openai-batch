import logging
import platform

from crontab import CronTab

from .. import runner
from ..db import schema
from .utils import cron_name

logger = logging.getLogger(__name__)


def unregister_task_unix(work_id: int) -> None:
    """Unregister the task from the crontab."""

    with CronTab() as cron:
        cron.remove_all(comment=cron_name(work_id))


def unregister_task_windows(work_id: int) -> None:
    pass


def unregister_task(work_id: int) -> None:
    match platform.system():
        case "Windows":
            unregister_task_windows(work_id)
        case "Linux" | "Darwin":
            unregister_task_unix(work_id)
        case other:
            logger.warning(f"Unsupported platform: {other}")


def to_completed(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    assert work.id is not None

    unregister_task(work.id)

    raise NotImplementedError()


def to_failed(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    assert work.id is not None

    unregister_task(work.id)

    raise NotImplementedError()


def to_canceled(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    assert work.id is not None

    unregister_task(work.id)

    raise NotImplementedError()
