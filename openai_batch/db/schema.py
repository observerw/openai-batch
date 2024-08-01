from datetime import datetime
from enum import Enum

from sqlmodel import JSON, Column, Field, Relationship, SQLModel


class WorkStatus(Enum):
    Created = "created"
    Checked = "checked"
    Completed = "completed"
    Failed = "failed"
    Canceled = "canceled"


class Work(SQLModel, table=True):
    # --------------------------------- Meta info -------------------------------- #

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )
    name: str | None = Field()

    # ------------------------------- Running info ------------------------------- #

    # required only when allow_same_dataset is False
    dataset_hash: str | None = Field(default=None, unique=True)
    status: WorkStatus = Field(default=WorkStatus.Created, index=True)

    # -------------------------------- resume info ------------------------------- #

    interpreter_path: str = Field()  # The interpreter path used to create the task
    script: str = Field()  # The full script of user defined Runner
    class_name: str = Field()  # The class name of user defined Runner
    work_dir: str = Field()  # The work directory of user defined Runner
    # use JSON column to store the id list
    undone_batch_ids: list[str] = Field(default=[], sa_column=Column(JSON))
    done_batch_ids: list[str] = Field(default=[], sa_column=Column(JSON))

    # ----------------------------- running processes ---------------------------- #
    processes: list["ProcessStatus"] = Relationship(back_populates="work")


class ProcessStatus(SQLModel, table=True):
    pid: int = Field(primary_key=True)

    work: Work | None = Relationship(back_populates="processes")

    description: str = Field()
    current: int = Field()
    total: int = Field()
