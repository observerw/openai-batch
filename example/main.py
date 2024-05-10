from datetime import timedelta
import json
import logging
from typing import Iterable

from openai_batch import (
    BatchErrorItem,
    BatchInputItem,
    BatchOutputItem,
    OpenAIBatchRunner,
)
from openai_batch.model import WorkConfig

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


class Runner(OpenAIBatchRunner):
    work_config = WorkConfig(
        name="example",
        completion_window=timedelta(hours=24),
        endpoint="/v1/chat/completions",
        allow_same_dataset=False,
        clean_up=True,
    )

    @staticmethod
    def upload() -> Iterable[BatchInputItem]:
        with open("data.jsonl", "r", encoding="utf-8") as f:
            # each line is a JSON object like {"id": "1", "content": "Hello!"}
            for line in f:
                data = json.loads(line)
                yield BatchInputItem(
                    id=data["id"],
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a helpful assistant.",
                        },
                        {
                            "role": "user",
                            "content": data["content"],
                        },
                    ],
                )

    @staticmethod
    def download(output: Iterable[BatchOutputItem]):
        with open("result.jsonl", "w", encoding="utf-8") as f:
            for item in output:
                if item.status == "success":
                    f.write(f"{item.model_dump_json()}\n")
                else:
                    logger.error(f"Request {item.id} failed: {item.error}")

    @staticmethod
    def download_error(output: Iterable[BatchErrorItem]):
        with open("errors.jsonl", "a", encoding="utf-8") as f:
            for item in output:
                f.write(f"{item.model_dump_json()}\n")


if __name__ == "__main__":
    Runner.run()
