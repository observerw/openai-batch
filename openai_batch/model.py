from datetime import timedelta
from typing import Iterable, Literal, Self, Union

from openai.types.batch import Batch
from openai.types.chat import ChatCompletion, ChatCompletionMessageParam
from openai.types.chat.chat_completion_tool_choice_option_param import (
    ChatCompletionToolChoiceOptionParam,
)
from openai.types.chat.chat_completion_tool_param import ChatCompletionToolParam
from openai.types.chat.completion_create_params import (
    CompletionCreateParamsNonStreaming as CompletionCreateParams,
)
from openai.types.chat.completion_create_params import ResponseFormat
from openai.types.chat_model import ChatModel
from pydantic import BaseModel, Field, model_validator

from .exception import OpenAIBatchException

type Endpoint = Literal["/v1/chat/completions", "/v1/embeddings", "/v1/completions"]


class WorkConfig(BaseModel):
    """Work configuration.

    Args:
        name (str, optional): Name of the work. Defaults to None.
        completion_window (timedelta, optional): Time window to wait for the completion. Defaults to 24 hours.
        check_interval (timedelta, optional): Interval to check the work status. Defaults to 4 hours.
        endpoint (Literal["/v1/chat/completions", "/v1/embeddings", "/v1/completions"], optional): Endpoint to use. Defaults to "/v1/chat/completions".
        allow_same_dataset (bool, optional): Allow the same dataset to be processed multiple times. Defaults to False.
        clean_up (bool, optional): Clean up the work after completion. Defaults to True.
    """

    name: str | None = None
    completion_window: timedelta = timedelta(hours=24)
    check_interval: timedelta = timedelta(hours=4)
    endpoint: Endpoint = "/v1/chat/completions"
    allow_same_dataset: bool = False
    clean_up: bool = True


class BatchInputItem(BaseModel):
    id: str
    messages: Iterable[ChatCompletionMessageParam]

    model: ChatModel = "gpt-3.5-turbo"
    frequency_penalty: float | None = Field(default=None, ge=-2.0, le=2.0)
    logit_bias: dict[str, int] | None = None
    logprobs: bool | None = None
    max_tokens: int | None = None
    n: int | None = 1
    presence_penalty: float | None = Field(default=None, ge=-2.0, le=2.0)
    response_format: ResponseFormat = {}
    seed: int | None = None
    stop: Union[str, list[str], None] = None
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    tool_choice: ChatCompletionToolChoiceOptionParam = "none"
    tools: Iterable[ChatCompletionToolParam] = Field(default=[], max_length=128)
    top_logprobs: int | None = Field(default=None, ge=0, le=20)
    top_p: float | None = Field(default=None, ge=0.0, le=1.0)
    user: str | None = None
    stream: Literal[False] | None = None

    @model_validator(mode="after")
    def _validate(self):
        if self.top_logprobs and not self.logprobs:
            raise ValueError("`top_logprobs` requires `logprobs` to be enabled")

        return self


class BatchOutputItem(BaseModel):
    batch_id: str
    id: str
    status: Literal["success", "failed"]
    response: str | None = None
    error: str | None = None


class BatchErrorItem(BaseModel):
    batch_id: str
    id: str

    code: str | None = None
    line: int | None = None
    message: str | None = None
    param: str | None = None


class BatchRequestInputItem(BaseModel):
    """
    Model of line in the input file for the batch request.

    https://platform.openai.com/docs/api-reference/batch/requestInput
    """

    custom_id: str
    method: Literal["POST"]
    url: Endpoint
    body: CompletionCreateParams

    @classmethod
    def from_input(
        cls,
        config: WorkConfig,
        item: BatchInputItem,
    ) -> "BatchRequestInputItem":
        return cls(
            custom_id=item.id,
            method="POST",
            url=config.endpoint,
            body=CompletionCreateParams(**item.model_dump(exclude={"id"})),
        )


class BatchRequestOutputItem(BaseModel):
    """
    Model of line in the output file for the batch request.

    https://platform.openai.com/docs/api-reference/batch/requestOutput
    """

    class Response(BaseModel):
        status_code: int
        request_id: str
        body: ChatCompletion

    class Error(BaseModel):
        code: str
        message: str

    id: str
    custom_id: str
    response: Response | None = None
    error: Error | None = None

    def to_output(self) -> BatchOutputItem:
        if self.error:
            error_message = f"Request failed with error code {self.error.code}: {self.error.message}"
        elif (resp := self.response) and resp.status_code != 200:
            error_message = f"Request failed with HTTP status code {resp.status_code}"
        else:
            error_message = None

        if not error_message and self.response:
            response = self.response.body.choices[0].message.content
        else:
            response = None

        status = "success" if not error_message else "failed"

        return BatchOutputItem(
            batch_id=self.custom_id,
            id=self.id,
            status=status,
            response=response,
            error=error_message,
        )

    def to_error_output(self) -> BatchErrorItem | None:
        if self.error:
            return BatchErrorItem(
                batch_id=self.custom_id,
                id=self.id,
                code=self.error.code,
                message=self.error.message,
            )

        return None


class BatchStatus(BaseModel):
    """
    Status object that extracts information from a `Batch`.
    """

    batch: Batch
    """Batch object, None only when the batch is not found"""

    @property
    def file_id(self) -> str | None:
        """
        When status is completed, the output_file_id; or when status is failed, the error_file_id
        """
        match (self.batch, self.status):
            case (
                (Batch(output_file_id=file_id), "success")
                | (Batch(error_file_id=file_id), "failed")
            ):
                return file_id
            case _:
                return None

    @property
    def status(self) -> Literal["success", "in_progress", "failed"]:
        match self.batch:
            case Batch(status="completed"):
                return "success"
            case Batch(status="failed" | "cancelled" | "expired"):
                return "failed"
            case Batch():  # validating, in_progress, finalizing, cancelling
                return "in_progress"

    @property
    def batch_id(self) -> str:
        return self.batch.id

    @model_validator(mode="after")
    def _validate(self) -> Self:
        match self:
            case BatchStatus(status="success" | "failed" as status, file_id=None):
                raise OpenAIBatchException(
                    f"Batch ended with status {status}"
                    "but corresponding file is missing"
                )

        return self
