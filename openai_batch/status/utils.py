import os
import types
from typing import Callable, Iterable

from pydantic import BaseModel

from .. import runner
from ..db import works_db
from ..model import BatchErrorItem, BatchOutputItem
from ..openai import openai_file


def cron_name(work_id: int) -> str:
    return f"openai_batch_{work_id}"


def _concat[T](gens: Iterable[Iterable[T]]) -> Iterable[T]:
    for gen in gens:
        yield from gen


def _download[T: BaseModel](
    file_ids: Iterable[str],
    model: type[T],
    download: Callable[[Iterable[T]], None],
):
    pid = os.getpid()

    def transform_file(file_id: str):
        for chunk in openai_file.retrieve(file_id):
            works_db.update_process_status(
                pid=pid,
                description="",
                status=chunk,
            )

            yield model.model_validate_json(chunk.line)

    download(_concat(transform_file(file_id) for file_id in file_ids))


def download(cls: type["runner.OpenAIBatchRunner"], output_file_ids: Iterable[str]):
    _download(output_file_ids, BatchOutputItem, cls.download)


def download_error(
    cls: type["runner.OpenAIBatchRunner"],
    error_file_ids: Iterable[str],
):
    _download(error_file_ids, BatchErrorItem, cls.download_error)


def load_cls(script: str, cls_name: str) -> type["runner.OpenAIBatchRunner"]:
    mod = types.ModuleType("mod")
    exec(script, mod.__dict__)

    return getattr(mod, cls_name)
