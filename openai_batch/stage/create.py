import inspect
import logging
import os
import sys
from pathlib import Path

from .. import runner
from ..db import schema, works_db

logger = logging.getLogger(__name__)


def create_work(cls: type["runner.OpenAIBatchRunner"]) -> schema.Work:
    cwd = os.getcwd()

    source_file_path = inspect.getsourcefile(cls)
    assert source_file_path is not None
    script = Path(source_file_path).read_text()
    interpreter_path = sys.executable

    work_config = cls.work_config

    db_work = works_db.create_work(
        schema.Work(
            name=work_config.name,
            completion_window=work_config.completion_window.seconds,
            endpoint=work_config.endpoint,
            allow_same_dataset=work_config.allow_same_dataset,
            clean_up=work_config.clean_up,
            interpreter_path=interpreter_path,
            work_dir=cwd,
            class_name=cls.__name__,
            script=script,
        ),
    )
    assert db_work.id is not None

    return db_work


def create_stage():
    raise NotImplementedError()
