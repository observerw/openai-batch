from datetime import datetime
from enum import Enum

from sqlmodel import Field, SQLModel, Column, JSON


class WorkStatus(Enum):
    Pending = "pending"
    Running = "running"
    Completed = "completed"
    Failed = "failed"


class Work(SQLModel, table=True):
    # Meta info
    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )

    # Running info
    dataset_hash: str | None = Field(default=None, unique=True)
    status: WorkStatus = Field(default=WorkStatus.Pending)
    pid: int | None = Field(default=None)

    # work config (required)
    name: str | None = Field()
    completion_window: int = Field()  # timedelta in second
    endpoint: str = Field()
    allow_same_dataset: bool = Field()
    clean_up: bool = Field()

    # resume info
    interpreter_path: str
    work_dir: str
    class_name: str
    script: str
    undone_batch_ids: set[str] = Field(default=set(), sa_column=Column(JSON))
    done_batch_ids: set[str] = Field(default=set(), sa_column=Column(JSON))