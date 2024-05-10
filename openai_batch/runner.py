from abc import abstractmethod
from typing import Iterable

from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)
from .worker import run_worker


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
        work = run_worker(cls)
        print(f"work {work.id} started")
