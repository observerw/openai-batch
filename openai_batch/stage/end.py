import logging
from typing import Iterable

import openai

from ..db import schema
from ..model import (
    BatchErrorItem,
    BatchRequestOutputItem,
)

logger = logging.getLogger(__name__)


def unregister_task_windows(id: int):
    raise NotImplementedError()


def unregister_task_unix(id: int):
    raise NotImplementedError()


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


def end_stage(work: schema.Work):
    raise NotImplementedError()
