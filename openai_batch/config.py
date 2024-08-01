import contextlib
import os
import platform
from pathlib import Path
from typing import Final

import toml
from pydantic import BaseModel, ConfigDict

match platform.system():
    case "Windows":
        appdata = os.getenv("APPDATA")
        if appdata is None:
            raise ValueError("cannot find %APPDATA%")
        config_dir = Path(appdata) / "openai_batch"
    case "Linux":
        config_dir = Path.home() / ".config" / "openai_batch"
    case "Darwin":
        config_dir = Path.home() / "Library" / "Application Support" / "openai_batch"
    case other_system:
        raise NotImplementedError(f"Unsupported platform: {other_system}")

config_path: Final = config_dir / "config.toml"


class OpenAIBatchConfig(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    save_path: str = str(Path.home() / ".openai_batch")

    @property
    def db_path(self) -> Path:
        return Path(self.save_path) / "works.sqlite"

    @classmethod
    def _load(cls) -> "OpenAIBatchConfig":
        if not config_path.exists():
            os.makedirs(config_dir, exist_ok=True)
            config = cls()
            config._save()
            return config

        return cls.model_validate(toml.load(config_path))

    def _save(self) -> None:
        with config_path.open("w") as f:
            toml.dump(self.model_dump(), f)

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
    exit(1)
