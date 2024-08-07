from dataclasses import dataclass
from typing import IO, Callable, Iterable, Literal

import requests as rq
from openai import OpenAI
from openai.types.file_object import FileObject
from requests_toolbelt import (
    MultipartEncoder,
    MultipartEncoderMonitor,
)

from ..const import K
from .utils import check_file_size


@dataclass(frozen=True)
class StreamChunk:
    current: int
    total: int

    @property
    def percentage(self) -> float:
        return self.current / self.total * 100

    @property
    def done(self) -> bool:
        return self.current == self.total


@dataclass(frozen=True)
class UploadStatus(StreamChunk):
    pass


@dataclass(frozen=True)
class RetrieveChunk(StreamChunk):
    line: str


DEFAULT_RETRIEVE_CHUNK_SIZE = 16 * K


class OpenAIFile:
    """
    example:

    ```python
    client = OpenAI()
    file = OpenAIFile(client)

    def on_upload_chunk(self, status: UploadStatus):
        print(f"{status.current}/{status.total}")

    with open("file.txt", "rb") as f:
        file_obj = file.upload(f, on_upload_chunk=on_upload_chunk, chunk_size = 1024 * 1024)
        print(f"uploaded file: {file_obj.id}")
    ```
    """

    _client: OpenAI

    def __init__(self, client: OpenAI):
        self._client = client

        self.session = rq.sessions.Session()

    def _upload_base_url(self) -> str:
        return str(self._client.base_url.join("/files"))

    def _retrieve_base_url(self, file_id: str) -> str:
        return str(self._client.base_url.join(f"/files/{file_id}/content"))

    def _retrieve_meta_base_url(self, file_id: str) -> str:
        return str(self._client.base_url.join(f"/files/{file_id}"))

    @property
    def _auth_headers(self):
        return self._client.auth_headers

    def upload(
        self,
        file: IO[bytes],
        purpose: Literal["assistants", "batch", "fine-tune", "vision"],
        on_upload_chunk: Callable[[UploadStatus], None] | None = None,
    ) -> FileObject:
        file_size = check_file_size(file)

        data = MultipartEncoder(
            {
                "file": file,
                "purpose": purpose,
            }
        )

        def handle_monitor(monitor: MultipartEncoderMonitor):
            assert on_upload_chunk is not None
            on_upload_chunk(
                UploadStatus(
                    current=monitor.bytes_read,
                    total=file_size,
                )
            )

        if on_upload_chunk:
            data = MultipartEncoderMonitor(data, handle_monitor)

        resp = self.session.post(
            self._upload_base_url(),
            data=data,
            headers={
                "Content-Type": data.content_type,
                **self._auth_headers,
            },
        )

        return FileObject.model_validate(resp.json())

    def retrieve(
        self,
        file_id: str,
        chunk_size: int = DEFAULT_RETRIEVE_CHUNK_SIZE,
    ) -> Iterable[RetrieveChunk]:
        meta = self.retrieve_meta(file_id)
        resp = self.session.get(
            self._retrieve_base_url(file_id),
            params={"file_id": file_id},
            stream=True,
            headers=self._auth_headers,
        )

        current = 0
        total = int(meta.bytes)
        for line in resp.iter_lines(
            chunk_size=chunk_size,
            decode_unicode=True,
        ):
            current += len(line)
            yield RetrieveChunk(
                current=current,
                total=total,
                line=line,
            )

    def retrieve_meta(self, file_id: str) -> FileObject:
        resp = self.session.get(
            self._retrieve_meta_base_url(file_id),
            headers=self._auth_headers,
        )
        return FileObject.model_validate(resp.json())
