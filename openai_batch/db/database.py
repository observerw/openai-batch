from typing import Iterable

from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel, select

from ..model import Config
from . import schema


class OpenAIBatchDatabase:
    def __init__(self, config: Config, database: str) -> None:
        self.config = config
        self.engine = create_engine(f"sqlite:///{database}")
        SQLModel.metadata.create_all(self.engine)

    def create_work(self, work: schema.Work):
        with Session(self.engine) as session:
            if self.config.allow_same_dataset and (
                (
                    exist_work := session.exec(
                        select(schema.Work).where(
                            schema.Work.dataset_hash == work.dataset_hash
                        )
                    ).first()
                )
                is not None
            ):
                raise ValueError(
                    f"work with dataset hash: "
                    f"{exist_work.dataset_hash}"
                    "already exists"
                )

            session.add(work)
            session.commit()

    def list_works(self, statuses: set[str] | None = None) -> Iterable[schema.Work]:
        with Session(self.engine) as session:
            statement = select(schema.Work)

            if statuses:
                statement = statement.where(schema.Work.status in statuses)

            works = session.exec(statement).all()
            return works

    def delete_work(self, work_id: int):
        with Session(self.engine) as session:
            work = session.exec(
                select(schema.Work).where(schema.Work.id == work_id)
            ).one_or_none()

            if work:
                session.delete(work)

            session.commit()
