from ..db import schema


class StatusInterrupt(Exception):
    def __init__(self, status: schema.WorkStatus) -> None:
        super().__init__(status)
        self.status = status


class OpenAIBatchException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
