import hashlib
import importlib.resources
import logging
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Iterable

import openai

from openai_batch.db import schema

from .. import scripts
from ..const import CHUNK_SIZE, MAX_FILE_SIZE
from ..model import (
    BatchInputItem,
    BatchRequestInputItem,
    WorkConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    dataset_hash: str
    files: list[IO[bytes]]


def transform(
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


def upload(config: WorkConfig, files: list[IO[bytes]]) -> UploadResult:
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


def register_task_windows(work: schema.Work):
    assert work.id is not None

    try:
        ps_script = importlib.resources.read_text(scripts, "register-task.ps1")
        subprocess.run(
            [
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
                "taskName": str(work.id),
                "completionWindow": str(work.completion_window),
            },
            input=ps_script,
        ).check_returncode()
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to register task: {e}")


def register_task_unix(work: schema.Work):
    # using crontab
    assert work.id is not None

    raise NotImplementedError()


def start_stage(work: schema.Work):
    raise NotImplementedError()
