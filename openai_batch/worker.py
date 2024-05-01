import hashlib
import os
import tempfile
from datetime import datetime, timedelta
from time import sleep
from typing import IO, Iterable, Tuple

import daemon
import daemon.pidfile
import openai

from .const import CHUNK_SIZE, MAX_FILE_SIZE
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchRequestInputItem,
    BatchRequestOutputItem,
    BatchStatus,
    Config,
    Notification,
)
from .runner import OpenAIBatchRunner


class Worker:
    id: str
    batch_ids: set[str]
    cls: type[OpenAIBatchRunner]

    created: datetime
    completion_window: timedelta
    notification: Notification | None
    config: Config

    def __init__(
        self,
        cls: type[OpenAIBatchRunner],
        notification: Notification | None = None,
        completion_window: timedelta = timedelta(hours=24),
        config: Config = Config(),
    ) -> None:
        self.cls = cls
        batch_input = self.cls.upload()
        self.id, self.batch_ids = self.upload(batch_input)  # TODO exit on same id

        self.notification = notification
        self.created = datetime.now()
        self.completion_window = completion_window
        self.config = config

    def check(self) -> Iterable[BatchStatus]:
        batch_ids_to_retrieve = [*self.batch_ids]
        cursor = openai.batches.list(limit=100)
        statuses: list[BatchStatus] = []

        while cursor.has_next_page():
            cursor = cursor.get_next_page()
            for batch in cursor.data:
                if batch.id not in batch_ids_to_retrieve:
                    continue

                match batch.status:
                    case "completed":
                        status = "success"
                        file_id = batch.output_file_id
                    case "failed" | "cancelled" | "expired":
                        status = "failed"
                        file_id = batch.error_file_id
                    case _:
                        status = "in_progress"
                        file_id = None

                statuses.append(
                    BatchStatus(
                        batch_id=batch.id,
                        status=status,
                        message=batch.status,
                        file_id=file_id,
                    )
                )

                batch_ids_to_retrieve.remove(batch.id)
                if len(batch_ids_to_retrieve) == 0:
                    break

        if len(batch_ids_to_retrieve) > 0:
            for batch_id in batch_ids_to_retrieve:
                statuses.append(
                    BatchStatus(
                        batch_id=batch_id,
                        status="failed",
                        message="not_found",
                        file_id=None,
                    )
                )

        return statuses

    @staticmethod
    def upload(batch_input: Iterable[BatchInputItem]) -> Tuple[str, set[str]]:
        hasher = hashlib.sha1()

        temp_files: list[IO[bytes]] = []
        total_file_size = 0
        curr_file = tempfile.TemporaryFile(buffering=CHUNK_SIZE)
        curr_file_size = 0

        for item in batch_input:
            request_item = BatchRequestInputItem.from_input(item)
            json = f"{request_item.model_dump_json()}\n".encode()
            hasher.update(json)
            total_file_size += len(json)

            if curr_file_size + len(json) > MAX_FILE_SIZE:
                temp_files.append(curr_file)
                curr_file = tempfile.TemporaryFile()
                curr_file_size = 0

            curr_file.write(json)

        if curr_file_size > 0:
            temp_files.append(curr_file)

        id = hasher.hexdigest()

        files = [
            openai.files.create(
                file=file,
                purpose="batch",  # type: ignore
            )
            for file in temp_files
        ]

        for file in temp_files:
            file.close()

        batches = [
            openai.batches.create(
                input_file_id=file.id,
                completion_window="24h",
                endpoint="/v1/chat/completions",
            )
            for file in files
        ]

        return id, {batch.id for batch in batches}

    def download(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            items = (
                BatchRequestOutputItem.model_validate_json(line).to_output()
                for line in lines
            )
            self.cls.download(items)

    def download_error(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            # TODO
            items = (BatchErrorItem.model_validate_json(line) for line in lines)
            self.cls.download_error(items)

    def notify(self, message: str):
        raise NotImplementedError()

    def run_once(self):
        raise NotImplementedError()

    def run(self):
        while self.created + self.completion_window > datetime.now():
            sleep(60 * 60 * 2)
            self.run_once()

        # TODO


def run_worker(worker: Worker):
    cwd = os.path.dirname(os.path.realpath(__file__))
    with daemon.DaemonContext(working_directory=cwd):
        worker.run()
