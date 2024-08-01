from dataclasses import dataclass
from typing import IO, Callable, Iterable

import requests as rq
from openai import OpenAI
from openai.types.file_object import FileObject

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
    data: bytes

    def decode(self, encoding: str = "utf-8") -> str:
        return self.data.decode(encoding)


DEFAULT_UPLOAD_CHUNK_SIZE = 16 * K
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
        data: IO[bytes],
        chunk_size: int = DEFAULT_UPLOAD_CHUNK_SIZE,
        on_upload_chunk: Callable[[UploadStatus], None] | None = None,
    ) -> FileObject:
        file_size = check_file_size(data)

        def chunked_upload() -> Iterable[bytes]:
            current = 0
            while True:
                chunk_data = data.read(chunk_size)
                current += len(chunk_data)

                if not chunk_data:
                    break

                if on_upload_chunk:
                    on_upload_chunk(UploadStatus(current, file_size))

                yield chunk_data

        resp = self.session.post(
            self._upload_base_url(),
            data=chunked_upload(),
            headers={
                "Content-Type": "application/octet-stream",
                **self._auth_headers,
            },
        )

        return FileObject.model_validate_json(resp.json())

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
        for chunk in resp.iter_content(chunk_size):
            current += len(chunk)
            yield RetrieveChunk(
                current=current,
                total=total,
                data=chunk,
            )

    def retrieve_meta(self, file_id: str) -> FileObject:
        resp = self.session.get(
            self._retrieve_meta_base_url(file_id),
            headers=self._auth_headers,
        )
        return FileObject.model_validate_json(resp.json())
