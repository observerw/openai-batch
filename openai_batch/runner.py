import os
from abc import abstractmethod
from datetime import timedelta
from typing import Iterable, Literal

from .config import OpenAIBatchConfig
from .db.database import OpenAIBatchDatabase
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)
from .worker import run_worker


class OpenAIBatchRunner:
    def __init__(
        self,
        openai_key: str | None = None,
        name: str | None = None,
        completion_window: timedelta = timedelta(hours=24),
        endpoint: Literal[
            "/v1/chat/completions",
            "/v1/embeddings",
        ] = "/v1/chat/completions",
        allow_same_dataset: bool = False,
        clean_up: bool = True,
    ) -> None:
        if openai_key:
            os.environ["OPENAI_KEY"] = openai_key

        self.config = OpenAIBatchConfig.load()
        self.work_config = WorkConfig(
            name=name,
            completion_window=completion_window,
            endpoint=endpoint,
            allow_same_dataset=allow_same_dataset,
            clean_up=clean_up,
        )
        self.db = OpenAIBatchDatabase(self.config.db_path)

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

    def run(self):
        work = run_worker(self.__class__, self.work_config, self.db)
        print(f"work {work.id} started")
