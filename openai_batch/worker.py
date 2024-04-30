import os
from dataclasses import KW_ONLY, dataclass
from datetime import datetime, timedelta
from multiprocessing import Process
from time import sleep
from typing import Iterable, Literal

import daemon
import openai

from .model import BatchErrorItem, BatchRequestOutputItem, BatchStatus
from .runner import OpenAIBatchRunner


@dataclass
class Worker:
    _: KW_ONLY

    id: str
    batch_ids: set[str]

    cls: type[OpenAIBatchRunner]
    created: datetime
    completion_window: timedelta = timedelta(hours=24)

    notify_method: Literal["email", "webhook"] | None = None
    address: str | None = None

    def check(self) -> Iterable[BatchStatus]:
        batch_ids_to_retrieve = [*self.batch_ids]
        cursor = openai.batches.list(limit=100)
        statuses: list[BatchStatus] = []

        while cursor.has_next_page():
            cursor = cursor.get_next_page()
            for batch in cursor.data:
                if batch.id not in batch_ids_to_retrieve:
                    continue

                match batch.status:
                    case "completed":
                        status = "success"
                        file_id = batch.output_file_id
                    case "failed" | "cancelled" | "expired":
                        status = "failed"
                        file_id = batch.error_file_id
                    case _:
                        status = "in_progress"
                        file_id = None

                statuses.append(
                    BatchStatus(
                        batch_id=batch.id,
                        status=status,
                        message=batch.status,
                        file_id=file_id,
                    )
                )

                batch_ids_to_retrieve.remove(batch.id)
                if len(batch_ids_to_retrieve) == 0:
                    break

        if len(batch_ids_to_retrieve) > 0:
            for batch_id in batch_ids_to_retrieve:
                statuses.append(
                    BatchStatus(
                        batch_id=batch_id,
                        status="failed",
                        message="not_found",
                        file_id=None,
                    )
                )

        return statuses

    def save(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            items = (
                BatchRequestOutputItem.model_validate_json(line).to_output()
                for line in lines
            )
            self.cls.download(items)

    def save_error(self, file_ids: Iterable[str]):
        for file_id in file_ids:
            content = openai.files.content(file_id)
            lines = (line for line in content.iter_lines())
            # TODO
            items = (BatchErrorItem.model_validate_json(line) for line in lines)
            self.cls.download_error(items)

    def notify(self, message: str):
        raise NotImplementedError()

    def run_once(self):
        raise NotImplementedError()

    def run(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)

        while True:
            sleep(60 * 60 * 2)
            self.run_once()


def run_worker(worker: Worker):
    with daemon.DaemonContext(pidfile=f"/var/run/OpenAIBatch-{worker.id}.pid"):
        p = Process(target=worker.run)
        p.start()
        p.join()
