import inspect
import logging
import os
import sys
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Any, Iterable

import pidfile
from rich.console import Console

from .config import config_dir
from .const import TO_STATUS, WORK_ID
from .db import schema, works_db
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)
from .status.status import to_status
from .utils import timestamp

console = Console()
logger = logging.getLogger(__name__)


class _RunnerMeta(ABCMeta, type):
    _derived = None

    def __new__(
        mcls: type["_RunnerMeta"],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ):
        new_cls = super().__new__(mcls, name, bases, namespace)
        for base in bases:
            if isinstance(base, _RunnerMeta):
                base._derived = new_cls
                break

        return new_cls


class OpenAIBatchRunner(metaclass=_RunnerMeta):
    work_config: WorkConfig = WorkConfig()

    @staticmethod
    @abstractmethod
    def upload() -> Iterable[BatchInputItem]:
        """
        Transform your own dataset into OpenAI Batch input format.
        """

    @staticmethod
    @abstractmethod
    def download(output: Iterable[BatchOutputItem]):
        """
        Transform OpenAI Batch output into your own dataset format.
        """

    @staticmethod
    def download_error(output: Iterable[BatchErrorItem]) -> None:
        """
        Save errors to a file.
        """

        return

    @classmethod
    def run(cls):
        match os.environ.get(WORK_ID):
            case str() as work_id if work_id.isdigit():  # invoke by running work or cli
                work = works_db.get_work(int(work_id))
                if work is None:
                    logger.error(f"Work with id {work_id} not found")
                    exit(1)
            case None:  # invoke by user
                work = create_work(cls)
            case id:
                logger.error(f"Unexpected work id: {id}")
                exit(1)

        pidfile_path = config_dir / f"{work.id}-{timestamp()}.pid"

        with pidfile.PIDFile(pidfile_path):
            status = (
                schema.WorkStatus(status)
                if (status := os.environ.get(TO_STATUS))
                else schema.WorkStatus.Checked
            )
            to_status(work, status, cls)


def create_work(cls: type[OpenAIBatchRunner]) -> schema.Work:
    source_file_path = inspect.getsourcefile(cls)
    assert source_file_path is not None
    source_file_path = Path(source_file_path)

    script = source_file_path.read_text()
    interpreter_path = sys.executable
    cwd = source_file_path.parent

    db_work = works_db.create_work(
        schema.Work(
            name=cls.work_config.name,
            status=schema.WorkStatus.Created,  # 默认状态为暂停，等同于未开始
            interpreter_path=interpreter_path,
            work_dir=str(cwd),
            class_name=cls.__name__,
            script=script,
        ),
    )
    assert db_work.id is not None

    return db_work
