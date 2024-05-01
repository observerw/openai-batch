from typing import Iterable, Literal, Union

from openai.types.batch_error import BatchError
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
from pydantic import BaseModel, EmailStr, Field, model_validator
from pydantic_core import Url


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

    @model_validator(mode="after")
    def _validate(self):
        if self.top_logprobs and not self.logprobs:
            raise ValueError("top_logprobs requires logprobs to be enabled")

        return self


class BatchOutputItem(BaseModel):
    batch_id: str
    id: str
    status: Literal["success", "failed"]
    response: str | None = None
    error: str | None = None


class BatchErrorItem(BatchError):
    batch_id: str
    id: str


class BatchRequestInputItem(BaseModel):
    """
    Model of line in the input file for the batch request.

    https://platform.openai.com/docs/api-reference/batch/requestInput
    """

    custom_id: str
    method: Literal["POST"] = "POST"
    url: Literal["/v1/chat/completions"] = "/v1/chat/completions"
    body: CompletionCreateParams

    @classmethod
    def from_input(cls, item: BatchInputItem) -> "BatchRequestInputItem":
        return cls(
            custom_id=item.id,
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
        code: int
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


class Config(BaseModel):
    exit_on_duplicate: bool = True


class Notification(BaseModel):
    method: Literal["email", "webhook"]
    address: Url | EmailStr

    @model_validator(mode="after")
    def _validate(self):
        match (self.method, self.address):
            case ("email", EmailStr()) | ("webhook", Url()):
                pass
            case _:
                raise ValueError("Invalid notification method and address combination")

        return self


class BatchStatus(BaseModel):
    batch_id: str
    status: Literal["success", "in_progress", "failed"]
    message: str | None = None
    # when status is completed, the output_file_id;
    # when status is failed, the error_file_id
    file_id: str | None = None
