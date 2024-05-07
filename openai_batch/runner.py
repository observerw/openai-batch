import argparse
from datetime import timedelta
import os
from abc import abstractmethod
from typing import Iterable

from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    Config,
)
from .worker import run_worker


class OpenAIBatchRunner:
    def __init__(
        self,
        openai_key: str | None = None,
        completion_window: timedelta = timedelta(hours=24),
        exit_on_duplicate: bool = True,
    ) -> None:
        if openai_key:
            os.environ["OPENAI_KEY"] = openai_key

        self.config = Config(
            completion_window=completion_window,
            exit_on_duplicate=exit_on_duplicate,
        )

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

    def _run(self):
        run_worker(self.__class__, self.config)

    def _list(self):
        raise NotImplementedError()

    def _remove(self, batch_id: str):
        raise NotImplementedError()

    def _status(self, batch_id: str):
        raise NotImplementedError()

    def cli(self):
        parser = argparse.ArgumentParser()

        args = parser.parse_args()

        # -l --list list all batch tasks
        parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            help="List all batches",
        )

        # -r --remove remove a batch task
        parser.add_argument(
            "-r",
            "--remove",
            type=str,
            help="Remove a batch",
        )

        # -s --status get the status of a batch task
        parser.add_argument(
            "-s",
            "--status",
            type=str,
            help="Get the status of a batch",
        )

        args = parser.parse_args()

        if not any(vars(args).values()):
            self._run()
        elif args.list:
            self._list()
        elif args.remove:
            self._remove(args.remove)
        elif args.status:
            self._status(args.status)
        else:
            parser.print_help()
            exit(1)
