import logging

from .. import runner
from ..db import schema, works_db
from .checked import to_checked
from .created import from_created
from .exception import OpenAIBatchException, StatusInterrupt
from .stopped import to_canceled, to_completed, to_failed
from .utils import load_cls

logger = logging.getLogger(__name__)


def to_status(
    work: schema.Work,
    status: schema.WorkStatus,
    cls: type["runner.OpenAIBatchRunner"] | None = None,
) -> schema.Work:
    """
    执行转移状态操作，执行完毕后将状态设置为想要转移到的状态。
    """

    prev_status = work.status
    if cls is None:
        cls = load_cls(work.script, work.class_name)

    try:
        match (prev_status, status):
            case (schema.WorkStatus.Created, schema.WorkStatus.Checked):
                from_created(work, cls=cls)
                to_checked(work, cls=cls)
            case (schema.WorkStatus.Checked, schema.WorkStatus.Checked):
                to_checked(work, cls=cls)
            case (_, schema.WorkStatus.Completed):
                to_completed(work, cls=cls)
            case (_, schema.WorkStatus.Failed):
                to_failed(work, cls=cls)
            case (_, schema.WorkStatus.Canceled):
                to_canceled(work, cls=cls)
            case _:
                raise OpenAIBatchException(
                    f"Invalid status transition: {prev_status} -> {status}"
                )

        assert work.id is not None
        with works_db.update_work(work_id=work.id) as work:
            work.status = status
            return work
    except StatusInterrupt as interrupt:  # switch to another stage
        return to_status(work, interrupt.status, cls=cls)
    except OpenAIBatchException as e:  # handle the exception
        logger.exception(f"Error occured: {e.message}")
        return to_status(work, schema.WorkStatus.Failed, cls=cls)
