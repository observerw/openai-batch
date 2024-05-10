from datetime import datetime
from typing import Annotated, Optional

import typer
from rich.console import Console

from openai_batch import schema

app = typer.Typer()
console = Console()


@app.command()
def get(id: Annotated[int, typer.Option(help="Work ID")]):
    print(id)


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
        typer.Option("--id", help="Work ID"),
    ] = None,
):
    console.print(created_at)
    console.print(statuses)
    console.print(names)
    console.print(ids)


@app.command()
def delete(id: Annotated[int, typer.Argument(help="Work ID")]):
    print(id)


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
    print(item)
    print(new_value)


if __name__ == "__main__":
    app()
