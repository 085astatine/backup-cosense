from __future__ import annotations
import dataclasses
import logging
import pathlib
from typing import Any, Optional, get_args
import dacite
import jsonschema
import toml
from ._backup import PageOrder


@dataclasses.dataclass(frozen=True)
class Config:
    scrapbox: ScrapboxConfig
    git: GitConfig


def jsonschema_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['scrapbox', 'git'],
        'additional_properties': False,
        'properties': {
            'scrapbox': jsonschema_scrapbox_config(),
            'git': jsonschema_git_config(),
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class ScrapboxConfig:
    project: str
    session_id: str
    save_directory: str


def jsonschema_scrapbox_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['project', 'session_id', 'save_directory'],
        'additionalProperties': False,
        'properties': {
            'project': {'type': 'string'},
            'session_id': {'type': 'string'},
            'save_directory': {'type': 'string'},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitConfig:
    path: str
    branch: Optional[str] = None
    page_order: Optional[PageOrder] = None


def jsonschema_git_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['path'],
        'additionalProperties': False,
        'properties': {
            'path': {'type': 'string'},
            'branch': {'type': 'string'},
            'page_order': {
                'type': ['string', 'null'],
                'enum': [None, *get_args(PageOrder)],
            },
        },
    }
    return schema


def load_config(
        path: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None) -> Config:
    logger = logger or logging.getLogger(__name__)
    # load TOML
    logger.info(f'load config from "{path.as_posix()}"')
    with path.open(encoding='utf-8') as file:
        loaded = toml.load(file)
    logger.debug(f'loaded toml: {repr(loaded)}')
    # JSON Schema validation
    jsonschema.validate(
            instance=loaded,
            schema=jsonschema_config())
    config = dacite.from_dict(
            data_class=Config,
            data=loaded,
            config=dacite.Config(strict=True))
    logger.debug(f'config: {repr(config)}')
    return config
