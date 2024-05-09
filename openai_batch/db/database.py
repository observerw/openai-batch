import contextlib
from pathlib import Path
from typing import Iterable

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel, select

from . import schema


class OpenAIBatchDatabase:
    def __init__(self, database: Path) -> None:
        self.engine = create_engine(f"sqlite:///{database}")
        SQLModel.metadata.create_all(self.engine)

    def create_work(self, work: schema.Work) -> schema.Work:
        with Session(self.engine) as session:
            session.add(work)
            session.commit()

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
            try:
                work = session.exec(
                    select(schema.Work).where(schema.Work.id == work_id)
                ).one()
            except sqlalchemy.exc.NoResultFound:
                raise ValueError(f"work with id: {work_id} not found")

            yield work

            session.add(work)
            session.commit()

    def update_work_status(self, work_id: int, status: schema.WorkStatus):
        with self.update_work(work_id) as work:
            work.status = status
