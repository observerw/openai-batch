import hashlib
import logging
import os
import signal
import tempfile
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import IO, Iterable

import daemon
import openai
import sqlalchemy
import sqlalchemy.exc

from openai_batch.db import schema

from . import runner
from .const import CHUNK_SIZE, MAX_FILE_SIZE
from .db import works_db
from .model import (
    BatchErrorItem,
    BatchInputItem,
    BatchRequestInputItem,
    BatchRequestOutputItem,
    BatchStatus,
    WorkConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    dataset_hash: str
    files: list[IO[bytes]]


def _transform(
    config: WorkConfig,
    batch_input: Iterable[BatchInputItem],
) -> TransformResult:
    hasher = hashlib.sha1()

    files: list[IO[bytes]] = []
    curr_file = tempfile.TemporaryFile(buffering=CHUNK_SIZE)
    curr_file_size = 0

    for item in batch_input:
        request_item = BatchRequestInputItem.from_input(config, item)
        json = f"{request_item.model_dump_json()}\n".encode()
        hasher.update(json)

        if curr_file_size + len(json) > MAX_FILE_SIZE:
            curr_file.seek(0)
            curr_file.flush()
            files.append(curr_file)
            curr_file = tempfile.TemporaryFile()
            curr_file_size = 0

        curr_file_size += len(json)
        curr_file.write(json)

    if curr_file_size > 0:
        curr_file.seek(0)
        files.append(curr_file)

    dataset_hash: str = hasher.hexdigest()
    return TransformResult(dataset_hash=dataset_hash, files=files)


@dataclass
class UploadResult:
    created: datetime
    batch_ids: set[str]


def _upload(config: WorkConfig, files: list[IO[bytes]]) -> UploadResult:
    uploaded_files = [
        openai.files.create(
            file=file,
            purpose="batch",
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
        created=datetime.now(),
        batch_ids={batch.id for batch in batches},
    )


def _check(batch_ids: set[str]) -> Iterable[BatchStatus]:
    cursor = openai.batches.list(limit=100)
    statuses: list[BatchStatus] = []

    for batch in cursor:
        if batch.id in batch_ids:
            statuses.append(BatchStatus.from_batch(batch))

        if len(statuses) == len(batch_ids):
            break

    found_ids = {status.batch_id for status in statuses}
    not_found_ids = batch_ids - found_ids

    for batch_id in not_found_ids:
        statuses.append(BatchStatus(message=f"batch with id {batch_id} not found"))

    return statuses


class Worker:
    def __init__(
        self,
        id: int,
        cls: type["runner.OpenAIBatchRunner"],
        created: datetime,
        undone_batch_ids: set[str] = set(),
        done_batch_ids: set[str] = set(),
    ) -> None:
        self.id = id
        self.cls = cls
        self.created = created
        self.undone_batch_ids = undone_batch_ids
        self.done_batch_ids = done_batch_ids

    @property
    def batch_ids(self) -> set[str]:
        return self.undone_batch_ids | self.done_batch_ids

    def done(self, batch_id: str):
        self.undone_batch_ids.discard(batch_id)
        self.done_batch_ids.add(batch_id)

    def download(self, output_file_ids: Iterable[str]):
        for file_id in output_file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            items = (
                BatchRequestOutputItem.model_validate_json(line).to_output()
                for line in lines
            )
            self.cls.download(items)

    def download_error(self, error_file_ids: Iterable[str]):
        for file_id in error_file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            # TODO
            items = (BatchErrorItem.model_validate_json(line) for line in lines)
            self.cls.download_error(items)

    def start(self):
        with works_db.update_work(self.id) as work:
            work.pid = os.getpid()
            work.status = schema.WorkStatus.Running

    def run_once(self):
        statuses = _check(self.undone_batch_ids)

        output_file_ids: list[str] = []
        error_file_ids: list[str] = []

        for status in statuses:
            match status:
                case BatchStatus(
                    batch_id=str() as batch_id,
                    status="success",
                    file_id=str() as file_id,
                ):
                    output_file_ids.append(file_id)
                    self.done(batch_id)
                    logger.info(
                        f"batch {batch_id} completed, remaining: "
                        f"{len(self.undone_batch_ids)}/{len(self.batch_ids)}"
                    )
                case BatchStatus(
                    batch_id=str() as batch_id,
                    status="failed",
                    file_id=str() as file_id,
                ):
                    error_file_ids.append(file_id)
                    self.done(batch_id)
                    logger.error(f"batch {batch_id} failed: {status.message}")
                case BatchStatus(message=message):
                    logger.error(f"unexpected batch status: {message}")

        self.download(output_file_ids)
        self.download_error(error_file_ids)

    def end(self, success: bool):
        logger.info(f"cleaning up work {self.id}")

        if self.cls.work_config.clean_up:
            file_ids = [
                file_id
                for status in _check(self.batch_ids)
                if (file_id := status.file_id) is not None
            ]

            for file_id in file_ids:
                openai.files.delete(file_id)

        if self.undone_batch_ids:
            logger.error(
                f"unexpected batches remaining: {self.batch_ids}, "
                "please manually check their status"
            )

        with works_db.update_work(self.id) as work:
            if success:
                work.status = schema.WorkStatus.Completed
            else:
                work.status = schema.WorkStatus.Failed
            work.pid = None

        logger.info(f"work {self.id} cleaned up")

    def pause(self):
        pass

    def run(self):
        logger.info(f"work {self.id} started")

        ddl = self.created + self.cls.work_config.completion_window

        self.start()

        try:
            while datetime.now() < ddl:
                sleep(60 * 60 * 2)  # TODO sleep interval
                self.run_once()
        except Exception as e:
            logger.error(f"work {self.id} failed: {e}")
            self.end(success=False)
        else:
            logger.info(f"work {self.id} completed")
            self.end(success=True)


def run_worker(cls: type["runner.OpenAIBatchRunner"]) -> schema.Work:
    work_config = cls.work_config
    db_work = works_db.create_work(
        schema.Work(
            name=work_config.name,
            completion_window=work_config.completion_window.seconds,
            endpoint=work_config.endpoint,
            allow_same_dataset=work_config.allow_same_dataset,
            clean_up=work_config.clean_up,
        ),
    )
    assert db_work.id is not None

    cwd = os.path.dirname(os.path.realpath(__file__))

    with daemon.DaemonContext(working_directory=cwd) as context:
        batch_input = cls.upload()
        transform_result = _transform(
            config=cls.work_config,
            batch_input=batch_input,
        )

        try:
            if not cls.work_config.allow_same_dataset:
                with works_db.update_work(db_work.id) as work:
                    work.dataset_hash = transform_result.dataset_hash
        except sqlalchemy.exc.IntegrityError:
            logger.error(f"work {db_work.id} process on same dataset")
            exit(-1)

        try:
            upload_result = _upload(
                config=cls.work_config,
                files=transform_result.files,
            )
        except openai.OpenAIError as e:
            logger.error(f"work {db_work.id} failed to upload: {e}")
            exit(-1)

        worker = Worker(
            id=db_work.id,
            cls=cls,
            created=upload_result.created,
            undone_batch_ids=upload_result.batch_ids,
        )

        context.signal_map = {
            signal.SIGTERM: worker.pause(),  # TODO pause on terminate
        }

        worker.run()

    return db_work


def resume_worker(work: schema.Work) -> schema.Work:
    # TODO resume worker from db
    raise NotImplementedError()
