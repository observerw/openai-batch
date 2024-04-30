from typing import Iterable
from archives.openai_batch.model import BatchErrorItem
from openai_batch.model import BatchInputItem, BatchOutputItem
from openai_batch.runner import OpenAIBatchRunner


class Runner(OpenAIBatchRunner):
    @staticmethod
    def upload() -> Iterable[BatchInputItem]:
        raise NotImplementedError()

    @staticmethod
    def download(output: Iterable[BatchOutputItem]):
        raise NotImplementedError()

    @staticmethod
    def download_error(output: Iterable[BatchErrorItem]) -> None:
        raise NotImplementedError()


if __name__ == "__main__":
    Runner().cli()
