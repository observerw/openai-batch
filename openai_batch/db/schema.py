from datetime import datetime
from enum import Enum

from sqlmodel import JSON, Column, Field, SQLModel


class WorkStatus(Enum):
    Created = "created"  # work is created but not started
    Running = "running"
    Completed = "completed"
    Failed = "failed"


class Work(SQLModel, table=True):
    # --------------------------------- Meta info -------------------------------- #
    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )

    # ------------------------------- Running info ------------------------------- #
    # required only when allow_same_dataset is False
    dataset_hash: str | None = Field(default=None, unique=True)
    status: WorkStatus = Field(default=WorkStatus.Created)
    pid: int | None = Field(default=None)

    # -------------------------- work config (required) -------------------------- #
    name: str | None = Field()
    completion_window: int = Field()  # timedelta in second
    endpoint: str = Field()
    allow_same_dataset: bool = Field()
    clean_up: bool = Field()

    # -------------------------------- resume info ------------------------------- #

    interpreter_path: str = Field()  # The interpreter path used to create the task
    work_dir: str = Field()
    class_name: str = Field()
    script: str = Field()  # The full script of user defined Runner
    # use JSON column to store the id list
    undone_batch_ids: list[str] = Field(default=[], sa_column=Column(JSON))
    done_batch_ids: list[str] = Field(default=[], sa_column=Column(JSON))
