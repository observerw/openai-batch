from ..db import schema
from .create import create_stage
from .end import end_stage
from .fail import fail_stage
from .running import running_stage
from .start import start_stage


def run_stage(work: schema.Work | None = None) -> schema.Work | None:
    try:
        match work:
            case None:
                return create_stage()
            case schema.Work(status=schema.WorkStatus.Created):
                return start_stage(work)
            case schema.Work(status=schema.WorkStatus.Running):
                return running_stage(work)
            case schema.Work(status=schema.WorkStatus.Running):
                return end_stage(work)
            case _:
                raise NotImplementedError()
    except Exception as e:
        fail_stage()
