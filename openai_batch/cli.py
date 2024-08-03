import subprocess as sp
import time
from datetime import datetime
from typing import Annotated, Iterable, Optional

import typer
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
from rich.text import Text

from .config import global_config
from .const import TO_STATUS, WORK_ID
from .db import schema, works_db
from .utils import recursive_getattr, recursive_setattr

app = typer.Typer()
console = Console()


def _colored_status(status: schema.WorkStatus) -> Text:
    match status:
        case schema.WorkStatus.Created:
            return Text(status.value, style="yellow")
        case schema.WorkStatus.Checked:
            return Text(status.value, style="blue")
        case schema.WorkStatus.Completed:
            return Text(status.value, style="green")
        case schema.WorkStatus.Failed | schema.WorkStatus.Canceled:
            return Text(status.value, style="red")
        case _:
            return Text(status.value)


def _show(works: Iterable[schema.Work]):
    table = Table()
    table.add_column("ID", style="cyan")
    table.add_column("name", style="magenta")
    table.add_column("Status")

    for work in works:
        table.add_row(str(work.id), work.name, _colored_status(work.status))

    console.print(table)


@app.command()
def get(id: Annotated[int, typer.Argument(help="Work ID")]):
    """Get work from ID."""

    work = works_db.get_work(id)
    if work is None:
        raise ValueError(f"Work with id: {id} not found")
        return

    _show([work])


@app.command()
def list(
    created_at: Annotated[
        Optional[datetime],
        typer.Option("--created-at", "-c", help="Earliest work create time"),
    ] = None,
    statuses: Annotated[
        Optional[list[schema.WorkStatus]],
        typer.Option("--status", "-s", help="Work status"),
    ] = None,
    names: Annotated[
        Optional[list[str]],
        typer.Option("--name", "-n", help="Work name"),
    ] = None,
    ids: Annotated[
        Optional[list[int]],
        typer.Option(help="Work ID"),
    ] = None,
):
    def accept(work: schema.Work) -> bool:
        if ids and work.id in ids:  # always True when id is specified
            return True

        if statuses and work.status not in statuses:
            return False

        if created_at and work.created_at < created_at:
            return False

        if names and (name := work.name) and (name not in names):
            return False

        return True

    _show([work for work in works_db.list_works() if accept(work)])


@app.command()
def delete(id: Annotated[int, typer.Argument(help="Work ID")]):
    work = works_db.delete_work(id)
    if work is None:
        raise ValueError(f"Work with id: {id} not found")

    sp.run(
        [
            work.interpreter_path,
            "-c",
            work.script,
        ],
        env={
            WORK_ID: str(work.id),
            TO_STATUS: str(schema.WorkStatus.Canceled),
        },
    ).check_returncode()

    _show([work])


@app.command()
def config(
    item: Annotated[
        str,
        typer.Argument(help="Config item"),
    ],
    new_value: Annotated[
        Optional[str],
        typer.Argument(help="New value"),
    ] = None,
):
    if new_value:
        with global_config.update() as config:
            recursive_setattr(config, item, new_value)
    else:
        value = recursive_getattr(global_config, item)
        console.print(f"{item}: {value}")


@app.command()
def inspect(id: Annotated[int, typer.Argument(help="Work ID")]):
    """
    Inspect current running processes of a work.
    """

    work = works_db.get_work(id)
    if work is None:
        raise ValueError(f"Work with id: {id} not found")

    with Progress() as progress:
        task_ids = [
            progress.add_task(
                process.description,
                total=process.total,
            )
            for process in work.processes
        ]
        pids = [process.pid for process in work.processes]

        while not progress.finished:
            with works_db.session() as session:
                for pid, task_id in zip(pids, task_ids):
                    process = session.get(schema.ProcessStatus, pid)
                    assert process is not None
                    progress.update(task_id, completed=process.current)

            time.sleep(1)
