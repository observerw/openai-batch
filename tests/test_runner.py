from typing import Iterable

from openai_batch.model import BatchInputItem, BatchOutputItem
from openai_batch.runner import OpenAIBatchRunner, create_work
from openai_batch.db import database


class RunnerTester(OpenAIBatchRunner):
    @staticmethod
    def upload() -> Iterable[BatchInputItem]:
        return []

    @staticmethod
    def download(output: Iterable[BatchOutputItem]):
        pass

def test_create_work():
    work = create_work(RunnerTester)
    assert work.id is not None
    assert work == database.works_db.get_work(work.id)
