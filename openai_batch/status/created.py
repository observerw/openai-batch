import hashlib
import importlib.resources
import logging
import platform
import subprocess as sp
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Iterable

import openai
import sqlalchemy
from crontab import CronTab

from .. import runner, scripts
from ..const import CHUNK_SIZE, MAX_FILE_SIZE
from ..db import schema, works_db
from ..model import (
    BatchInputItem,
    BatchRequestInputItem,
    WorkConfig,
)
from ..utils import to_minutes
from .exception import OpenAIBatchException
from .utils import cron_name

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    dataset_hash: str | None
    files: list[IO[bytes]]


def transform(
    config: WorkConfig,
    batch_input: Iterable[BatchInputItem],
) -> TransformResult:
    hash = not config.allow_same_dataset
    hasher = hashlib.sha1()

    files: list[IO[bytes]] = []
    curr_file = tempfile.TemporaryFile(buffering=CHUNK_SIZE)
    curr_file_size = 0

    for item in batch_input:
        request_item = BatchRequestInputItem.from_input(config, item)
        json = f"{request_item.model_dump_json()}\n".encode()
        if hash:
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

    return TransformResult(
        dataset_hash=hasher.hexdigest() if hash else None,
        files=files,
    )


@dataclass
class UploadResult:
    created: datetime
    batch_ids: set[str]


def upload(config: WorkConfig, files: list[IO[bytes]]) -> UploadResult:
    uploaded_files = [openai.files.create(file=file, purpose="batch") for file in files]

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


def register_task_windows(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
):
    assert work.id is not None

    try:
        script = importlib.resources.read_text(scripts, "register-work.ps1")
        sp.run(
            args=[
                "powershell.exe",
                "-ExecutionPolicy",
                "RemoteSigned",
                "-Command",
                "-",
            ],
            env={
                "interpreter": work.interpreter_path,
                "script": work.script,
                "workDir": work.work_dir,
                "workName": str(work.id),
                "checkInterval": str(cls.work_config.check_interval),
            },
            input=script,
        ).check_returncode()
    except sp.CalledProcessError as e:
        raise OpenAIBatchException(message=f"Failed to register work: {e}")


def register_task_unix(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
):
    assert work.id is not None
    config = cls.work_config
    with CronTab() as cron:
        job = cron.new(
            command=f"cd {work.work_dir} && {work.interpreter_path} -c {work.script}",
            comment=cron_name(work.id),
        )
        job.every(to_minutes(config.check_interval)).minutes()  # type: ignore


def from_created(
    work: schema.Work,
    cls: type["runner.OpenAIBatchRunner"],
) -> schema.Work:
    """
    Upload the batch input to OpenAI and register work in the system.
    """

    config = cls.work_config

    transform_result = transform(
        config=config,
        batch_input=cls.upload(),
    )

    if not config.allow_same_dataset:
        try:
            assert work.id is not None
            with works_db.update_work(work.id) as work:
                work.dataset_hash = transform_result.dataset_hash
        except sqlalchemy.exc.IntegrityError:
            raise OpenAIBatchException(
                message="The dataset has been used by another work"
            )

    upload_result = upload(
        config=config,
        files=transform_result.files,
    )

    assert work.id is not None
    with works_db.update_work(work.id) as work:
        work.created_at = upload_result.created
        work.undone_batch_ids = list(upload_result.batch_ids)

    match platform.system():
        case "Windows":
            register_task_windows(work, cls)
        case "Linux" | "Darwin":
            register_task_unix(work, cls)
        case other:
            raise NotImplementedError(f"Unsupported platform: {other}")

    return work
