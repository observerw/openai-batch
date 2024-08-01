from datetime import datetime, timedelta
from typing import Any


def recursive_setattr(obj: object, attr: str, value: Any):
    attrs = attr.split(".")
    match attrs:
        case [attr]:
            setattr(obj, attr, value)
        case [attr, *rest]:
            obj = getattr(obj, attr)
            recursive_setattr(obj, ".".join(rest), value)


def recursive_getattr(obj: object, attr: str) -> Any:
    attrs = attr.split(".")
    for attr in attrs:
        obj = getattr(obj, attr)
    return obj


def to_minutes(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def timestamp() -> int:
    return int(datetime.now().timestamp())
