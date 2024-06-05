import contextlib
import os
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel, select

from ..config import global_config
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

    def create_work(self, work: schema.Work) -> schema.Work:
        with Session(self.engine) as session:
            session.add(work)
            session.commit()

        return work

    def get_work(self, work_id: int) -> schema.Work | None:
        with Session(self.engine) as session:
            work = session.exec(
                select(schema.Work).where(schema.Work.id == work_id)
            ).one_or_none()

        return work

    def list_works(self, statuses: set[str] | None = None) -> Iterable[schema.Work]:
        with Session(self.engine) as session:
            statement = select(schema.Work)

            if statuses:
                statement = statement.where(schema.Work.status in statuses)

            works = session.exec(statement).all()

        return works

    def delete_work(self, work_id: int) -> schema.Work | None:
        with Session(self.engine) as session:
            work = session.exec(
                select(schema.Work).where(schema.Work.id == work_id)
            ).one_or_none()

            if work:
                session.delete(work)

            session.commit()

        return work

    @contextlib.contextmanager
    def update_work(self, work_id: int):
        with Session(self.engine) as session:
            work = session.exec(
                select(schema.Work).where(schema.Work.id == work_id)
            ).one_or_none()

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


try:
    works_db = OpenAIBatchDatabase(global_config.db_path)
except Exception as e:
    print(f"Failed to initialize database: {e}")
    exit(1)
