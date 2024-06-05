from typing import Iterable

import openai

from .. import runner
from ..model import (
    BatchErrorItem,
    BatchRequestOutputItem,
)
import types


def cron_name(work_id: int) -> str:
    return f"openai_batch_{work_id}"


def download(cls: type["runner.OpenAIBatchRunner"], output_file_ids: Iterable[str]):
    for file_id in output_file_ids:
        content = openai.files.content(file_id)
        lines = (line for line in content.iter_lines())
        items = (
            BatchRequestOutputItem.model_validate_json(line).to_output()
            for line in lines
        )
        cls.download(items)


def download_error(
    cls: type["runner.OpenAIBatchRunner"],
    error_file_ids: Iterable[str],
):
    for file_id in error_file_ids:
        content = openai.files.content(file_id)
        lines = (line for line in content.iter_lines())
        # TODO
        items = (BatchErrorItem.model_validate_json(line) for line in lines)
        cls.download_error(items)


def load_cls(script: str, cls_name: str) -> type["runner.OpenAIBatchRunner"]:
    mod = types.ModuleType("mod")
    exec(script, mod.__dict__)

    return getattr(mod, cls_name)
