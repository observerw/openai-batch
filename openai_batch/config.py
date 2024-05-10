import contextlib
import os
import platform
from pathlib import Path

import tomli_w
import tomllib
from pydantic import BaseModel, ConfigDict

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
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    save_path: Path = Path.home() / ".openai_batch"

    @property
    def db_path(self) -> Path:
        return self.save_path / "works.sqlite"

    @classmethod
    def _load(cls) -> "OpenAIBatchConfig":
        if not _config_path.exists():
            os.makedirs(_config_dir, exist_ok=True)
            config = cls()
            config._save()
            return config

        with _config_path.open("rb") as f:
            return cls.model_validate(tomllib.load(f))

    def _save(self) -> None:
        with _config_path.open("wb") as f:
            tomli_w.dump(self.model_dump(), f)

    @contextlib.contextmanager
    @staticmethod
    def update():
        config = OpenAIBatchConfig._load()
        try:
            yield config
        finally:
            global global_config
            global_config = config  # update global_config variable to keep it in sync
            config._save()


try:
    global_config = OpenAIBatchConfig._load()
except Exception as e:
    print(f"Failed to load config: {e}")
    exit(-1)
