import contextlib
import os
import platform
from pathlib import Path

import tomli_w
import tomllib
from pydantic import BaseModel

match platform.system():
    case "Windows":
        appdata = os.getenv("APPDATA")
        if appdata is None:
            raise ValueError("APPDATA environment variable is not set")
        _config_dir = Path(appdata) / "openai_batch"
    case "Linux":
        _config_dir = Path.home() / ".config" / "openai_batch"
    case "Darwin":
        _config_dir = Path.home() / "Library" / "Application Support" / "openai_batch"
    case _:
        raise NotImplementedError(f"Unsupported platform: {platform.system()}")

_config_path = _config_dir / "config.toml"


class OpenAIBatchConfig(BaseModel):
    save_path: Path = Path.home() / ".openai_batch"

    @property
    def db_path(self) -> Path:
        return self.save_path / "works.sqlite"

    @classmethod
    def load(cls) -> "OpenAIBatchConfig":
        if not _config_path.exists():
            _config_path.touch()
            return cls()

        with _config_path.open("rb") as f:
            return cls.model_validate(tomllib.load(f))

    def save(self) -> None:
        with _config_path.open("wb") as f:
            tomli_w.dump(self.model_dump(), f)

    @contextlib.contextmanager
    def update():
        config = OpenAIBatchConfig.load()
        try:
            yield config
        finally:
            config.save()
