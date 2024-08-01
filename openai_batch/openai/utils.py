import os
from typing import IO, Generator


class ValuedGenerator[T, U, V]:
    _value: V | None = None

    def __init__(self, gen: Generator[T, U, V]):
        self.gen = gen

    def __iter__(self) -> Generator[T, U, None]:
        self._value = yield from self.gen

    @property
    def value(self) -> V:
        if self._value is None:
            raise ValueError("generator not exhausted")

        return self._value


def check_file_size(data: IO[bytes]) -> int:
    data.seek(0, os.SEEK_END)
    size = data.tell()
    data.seek(0)

    return size
