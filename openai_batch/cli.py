import os
from datetime import datetime
from typing import Iterable

import click
import colors
import prettytable as pt

from .db import schema, works_db


def ansi_status(status: schema.WorkStatus) -> str:
    match status:
        case schema.WorkStatus.Pending:
            return colors.yellow(status.value)
        case schema.WorkStatus.Running:
            return colors.blue(status.value)
        case schema.WorkStatus.Completed:
            return colors.green(status.value)
        case schema.WorkStatus.Failed:
            return colors.red(status.value)


def _show(works: Iterable[schema.Work]):
    table = pt.PrettyTable()
    table.field_names = ["ID", "name", "Status"]
    for work in works:
        table.add_row(
            [
                work.id,
                work.name,
                ansi_status(work.status),
            ]
        )

    click.echo(table)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--id", type=int, help="Work ID")
def get(work_id: int):
    work = works_db.get_work(work_id)
    if work is None:
        click.echo(f"Work with id: {work_id} not found")
        return

    _show([work])


@cli.command()
def list(
    status: Iterable[schema.WorkStatus] | None = None,
    created_at: datetime | None = None,
    names: Iterable[str] | None = None,
):
    def accept(work: schema.Work) -> bool:
        if status and work.status not in status:
            return False

        if created_at and work.created_at < created_at:
            return False

        if names and (name := work.name) and (name not in names):
            return False

        return True

    works = [work for work in works_db.list_works() if accept(work)]

    _show(works)


@cli.command()
@click.option("--id", type=int, help="Work ID")
def delete(work_id: int):
    work = works_db.delete_work(work_id)
    if work is None:
        click.echo(f"Work with id: {work_id} not found")
        return

    if pid := work.pid:
        os.kill(pid, 9)

    click.echo(f"Deleted work with id: {work_id}")
