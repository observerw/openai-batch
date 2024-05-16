import logging
import os
from abc import abstractmethod
from typing import Iterable

from rich.console import Console

from .const import WORK_ID
from .db import works_db
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)
from .stage.stage import run_stage

console = Console()
logger = logging.getLogger(__name__)


class OpenAIBatchRunner:
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
            case str() as id if id.isdigit():
                work = works_db.get_work(int(id))
                if work is None:
                    logger.error(f"Work with id {id} not found")
                    exit(1)
                run_stage(work)
            case None:
                work = run_stage()
            case id:
                logger.error(f"Invalid work id: {id}")
                exit(1)
