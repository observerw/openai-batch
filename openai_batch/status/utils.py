import os
import types
from typing import Iterable, Sequence

from .. import runner
from ..db import works_db
from ..model import BatchRequestOutputItem
from ..openai import openai_file


def cron_name(work_id: int) -> str:
    return f"openai_batch_{work_id}"


def _concat[T](gens: Iterable[Iterable[T]]) -> Iterable[T]:
    """Merge multiple generators into one."""

    for gen in gens:
        yield from gen


def _download(file_ids: Sequence[str], progress: bool = False):
    pid = os.getpid()
    file_count = len(file_ids)

    def download_file(idx: int, file_id: str):
        for chunk in openai_file.retrieve(file_id):
            if progress:
                desc = f"Downloading file {idx + 1}/{file_count}"
                works_db.update_process_status(
                    pid=pid,
                    description=desc,
                    status=chunk,
                )

            yield BatchRequestOutputItem.model_validate_json(chunk.line)

    yield from _concat(
        download_file(idx, file_id)  #
        for idx, file_id in enumerate(file_ids)
    )


def download(
    cls: type["runner.OpenAIBatchRunner"],
    output_file_ids: Sequence[str],
):
    cls.download(
        item.to_output()  #
        for item in _download(output_file_ids)
    )


def download_error(
    cls: type["runner.OpenAIBatchRunner"],
    error_file_ids: Sequence[str],
):
    cls.download_error(
        output_item
        for item in _download(error_file_ids, progress=False)
        if (output_item := item.to_error_output()) is not None
    )


def load_cls(script: str, cls_name: str) -> type["runner.OpenAIBatchRunner"]:
    mod = types.ModuleType("mod")
    exec(script, mod.__dict__)

    return getattr(mod, cls_name)
