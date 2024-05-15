import os
from abc import abstractmethod
from typing import Iterable
from venv import logger

from rich.console import Console

from openai_batch.db import schema

from . import stage
from .const import WORK_ID
from .db import works_db
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)

console = Console()


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
                    logger.error(f"Work {id} not found.")
                    exit(-1)

                match work.status:
                    case schema.WorkStatus.Created:
                        stage.start(work)
                    case schema.WorkStatus.Running:
                        stage.running(work)
                    case schema.WorkStatus.Completed:
                        stage.end(work)
                    case other_status:
                        logger.error(f"Work {id} unexpected in {other_status} status.")

            case None:  # user start a new work
                work = stage.create()
                console.print("Work successfully started.", style="green")
            case _:
                logger.error(f"Invalid {WORK_ID}")
                exit(-1)
