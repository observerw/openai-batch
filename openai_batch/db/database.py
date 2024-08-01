import contextlib
import os
from pathlib import Path
from typing import Final, Iterable

from sqlmodel import Session, SQLModel, create_engine, select

from ..config import global_config
from ..openai.upload import StreamChunk
from . import schema


class OpenAIBatchDatabase:
    """
    A sqlite database for storing OpenAI Batch works.
    """

    def __init__(self, database: Path) -> None:
        if not database.exists():
            os.makedirs(database.parent, exist_ok=True)

        self.engine = create_engine(f"sqlite:///{database}")
        SQLModel.metadata.create_all(self.engine)

    @contextlib.contextmanager
    def session(self):
        with Session(self.engine) as session:
            yield session

    def create_work(self, work: schema.Work) -> schema.Work:
        with Session(self.engine) as session:
            session.add(work)
            session.commit()
            # Automatic attribute refresh must be bound to session
            session.refresh(work)

        return work

    def get_work(self, work_id: int) -> schema.Work | None:
        with Session(self.engine) as session:
            work = session.get(schema.Work, work_id)

        return work

    def list_works(
        self,
        statuses: set[schema.WorkStatus] | None = None,
    ) -> Iterable[schema.Work]:
        with Session(self.engine) as session:
            statement = select(schema.Work)

            if statuses:
                statement = statement.where(schema.Work.status in statuses)

            works = session.exec(statement).all()

        return works

    def delete_work(self, work_id: int) -> schema.Work | None:
        with Session(self.engine) as session:
            work = session.get(schema.Work, work_id)

            if work:
                session.delete(work)

            session.commit()

        return work

    @contextlib.contextmanager
    def update_work(self, work_id: int):
        with Session(self.engine) as session:
            work = session.get(schema.Work, work_id)

            if work is None:
                raise ValueError(f"work with id: {work_id} not found")

            yield work

            session.add(work)
            session.commit()

    def update_work_status(
        self,
        work_id: int,
        status: schema.WorkStatus,
    ) -> schema.Work:
        with self.update_work(work_id) as work:
            work.status = status

        return work

    def update_process_status(
        self,
        pid: int,
        description: str,
        status: "StreamChunk | None",
    ):
        with self.session() as session:
            process = session.get(schema.ProcessStatus, pid)

            match (status, process):
                # update existing process status
                case (StreamChunk() as status, schema.ProcessStatus() as process):
                    process.current = status.current
                    process.total = status.total
                    session.add(process)
                # delete process status
                case (None, schema.ProcessStatus() as process):
                    session.delete(process)
                # create new process status
                case (StreamChunk() as status, None):
                    process = schema.ProcessStatus(
                        pid=pid,
                        current=status.current,
                        total=status.total,
                        description=description,
                    )
                    session.add(process)
                case _:
                    pass

            session.commit()


try:
    works_db: Final = OpenAIBatchDatabase(global_config.db_path)
except Exception as e:
    print(f"Failed to initialize database: {e}")
    exit(1)
