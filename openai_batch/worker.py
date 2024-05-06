import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import IO, Callable, Iterable

import daemon
import daemon.pidfile
import openai
import pidfile

from . import runner
from .const import CHUNK_SIZE, MAX_FILE_SIZE
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    BatchRequestInputItem,
    BatchRequestOutputItem,
    BatchStatus,
    Config,
)

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    id: str
    files: list[IO[bytes]]


def transform(config: Config, batch_input: Iterable[BatchInputItem]) -> TransformResult:
    hasher = hashlib.sha1()

    files: list[IO[bytes]] = []
    total_file_size = 0
    curr_file = tempfile.TemporaryFile(buffering=CHUNK_SIZE)
    curr_file_size = 0

    for item in batch_input:
        request_item = BatchRequestInputItem.from_input(config, item)
        json = f"{request_item.model_dump_json()}\n".encode()
        hasher.update(json)
        total_file_size += len(json)

        if curr_file_size + len(json) > MAX_FILE_SIZE:
            files.append(curr_file)
            curr_file = tempfile.TemporaryFile()
            curr_file_size = 0

        curr_file.write(json)

    if curr_file_size > 0:
        files.append(curr_file)

    id = hasher.hexdigest()
    return TransformResult(id=id, files=files)


@dataclass
class UploadResult:
    created: datetime
    batch_ids: set[str]


def upload(config: Config, files: list[IO[bytes]]) -> UploadResult:
    uploaded_files = [
        openai.files.create(
            file=file,
            purpose="batch",  # type: ignore
        )
        for file in files
    ]

    # comp_window = config.completion_window
    batches = [
        openai.batches.create(
            input_file_id=file.id,
            # completion_window=f"{comp_window.days}d{comp_window.seconds}s",
            completion_window="24h",  # FIXME
            endpoint=config.endpoint,
        )
        for file in uploaded_files
    ]

    return UploadResult(
        batch_ids={batch.id for batch in batches},
        created=datetime.now(),
    )


class Worker:
    def __init__(
        self,
        config: Config,
        created: datetime,
        batch_ids: set[str],
        download: Callable[[Iterable[BatchOutputItem]], None],
        download_error: Callable[[Iterable[BatchErrorItem]], None],
    ) -> None:
        self.config = config
        self.created = created
        self.batch_ids = batch_ids
        self._download = download
        self._download_error = download_error

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

    def download(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            items = (
                BatchRequestOutputItem.model_validate_json(line).to_output()
                for line in lines
            )
            self._download(items)

    def download_error(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            # TODO
            items = (BatchErrorItem.model_validate_json(line) for line in lines)
            self._download_error(items)

    def run_once(self):
        raise NotImplementedError()

    def run(self):
        while self.created + self.config.completion_window > datetime.now():
            sleep(60 * 60 * 2)
            self.run_once()


def run_worker(cls: type["runner.OpenAIBatchRunner"], config: Config):
    cwd = os.path.dirname(os.path.realpath(__file__))
    with daemon.DaemonContext(working_directory=cwd):
        batch_input = cls.upload()
        transform_result = transform(config=config, batch_input=batch_input)
        id = transform_result.id

        def run():
            upload_result = upload(config=config, files=transform_result.files)
            Worker(
                config=config,
                created=upload_result.created,
                batch_ids=upload_result.batch_ids,
                download=cls.download,
                download_error=cls.download_error,
            ).run()

        if config.exit_on_duplicate:
            try:
                with pidfile.PIDFile(f"/var/run/OpenAI_Batch_{id}.pid"):
                    run()
            except pidfile.AlreadyRunningError:
                logger.error(f"work {id} is already running, exiting.")
                exit(0)
        else:
            run()
