from datetime import datetime
from enum import Enum

from sqlmodel import Field, SQLModel


class WorkStatus(Enum):
    Running = "running"
    Completed = "completed"
    Failed = "failed"


class Work(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str | None = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.now)
    dataset_hash: str = Field(unique=True)
    status: WorkStatus = Field(default=WorkStatus.Running)
    pid: int | None = Field(default=None)
