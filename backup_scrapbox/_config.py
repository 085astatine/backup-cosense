from __future__ import annotations
import dataclasses
import pathlib
from typing import Optional
import dacite
import toml
from ._backup import PageOrder


@dataclasses.dataclass(frozen=True)
class Config:
    scrapbox: ScrapboxConfig
    git: GitConfig


@dataclasses.dataclass(frozen=True)
class ScrapboxConfig:
    project: str
    session_id: str
    save_directory: str


@dataclasses.dataclass(frozen=True)
class GitConfig:
    path: str
    branch: Optional[str] = None
    page_order: Optional[PageOrder] = None


def load_config(path: pathlib.Path) -> Config:
    with path.open(encoding='utf-8') as file:
        loaded = toml.load(file)
    return dacite.from_dict(
            data_class=Config,
            data=loaded,
            config=dacite.Config(strict=True))
