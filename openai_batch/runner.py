import inspect
import os
import subprocess
import sys
from abc import abstractmethod
from pathlib import Path
from typing import Iterable

from rich.console import Console

from .db import schema, works_db
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    WorkConfig,
)
from .worker import resume_worker

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
    def _create_work(cls) -> schema.Work:
        cwd = os.path.dirname(os.path.realpath(__file__))

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

    @classmethod
    def run(cls):
        match os.environ.get("OPENAI_BATCH_RESUME_ID"):
            # run from user, create a new work and run it in a subprocess
            case None:
                work = cls._create_work()
                assert work.id is not None
                id = work.id

                subprocess.run(
                    [
                        f"OPENAI_BATCH_RESUME_ID={work.id}",
                        work.interpreter_path,
                        "-c",
                        work.script,
                    ],
                    start_new_session=True,  # TODO Windows compatibility
                )

            # run from subprocess, resume the work with OPENAI_BATCH_RESUME_ID
            case str() as id if id.isdigit():
                work = works_db.get_work(int(id))
                if not work:
                    console.print(f"Work with id: {id} not found", style="bold red")
                    sys.exit(-1)

                resume_worker(work)
            case _:
                console.print("Invalid OPENAI_BATCH_RESUME_ID.", style="bold red")
                sys.exit(-1)

        console.print(f"Work {id} successfully started.", style="green")
