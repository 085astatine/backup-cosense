import dataclasses
import logging
import pathlib
from typing import Any, Literal, Optional, get_args
import dacite
import jsonschema
import toml
from ._backup import PageOrder
from ._git import Git


@dataclasses.dataclass(frozen=True)
class ScrapboxConfig:
    project: str
    session_id: str
    save_directory: str
    request_interval: float = 3.0


def jsonschema_scrapbox_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['project', 'session_id', 'save_directory'],
        'additionalProperties': False,
        'properties': {
            'project': {'type': 'string'},
            'session_id': {'type': 'string'},
            'save_directory': {'type': 'string'},
            'request_interval': {
                'type': 'number',
                'exclusiveMinimum': 0.0,
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitConfig:
    path: str
    branch: Optional[str] = None
    page_order: Optional[PageOrder] = None
    user_name: Optional[str] = None
    user_email: Optional[str] = None

    def git(self,
            *,
            logger: Optional[logging.Logger] = None) -> Git:
        return Git(
                pathlib.Path(self.path),
                branch=self.branch,
                logger=logger)


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
            'user_name': {
                'type': ['string', 'null'],
            },
            'user_email': {
                'type': ['string', 'null'],
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class ExternalLinkConfig:
    # pylint: disable=too-many-instance-attributes
    enabled: bool = False
    use_git_lfs: bool = False
    log_directory: str = 'log'
    save_directory: str = 'links'
    parallel_limit: int = 5
    parallel_limit_per_host: int = 0
    request_interval: float = 1.0
    request_headers: dict[str, str] = dataclasses.field(default_factory=dict)
    timeout: float = 30.0
    content_types: list[str] = dataclasses.field(default_factory=list)
    excluded_urls: list[str] = dataclasses.field(default_factory=list)
    allways_request_all_links: bool = False
    keep_logs: int | Literal['all'] = 'all'
    keep_deleted_links: bool = False


def jsonschema_external_link_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'enabled': {'type': 'boolean'},
            'use_git_lfs': {'type': 'boolean'},
            'log_directory': {'type': 'string'},
            'save_directory': {'type': 'string'},
            'parallel_limit': {
                'type': 'integer',
                'minimum': 1,
            },
            'parallel_limit_per_host': {
                'type': 'integer',
                'minimum': 0,
            },
            'request_interval': {
                'type': 'number',
                'exclusiveMinimum': 0.0,
            },
            'request_headers': {
                'type': 'object',
                'additionalProperties': {
                    'type': 'string',
                },
            },
            'timeout': {
                'type': 'number',
                'exclusiveMinimum': 0.0,
            },
            'content_types': {
                'type': 'array',
                'items': {'type': 'string'},
            },
            'excluded_urls': {
                'type': 'array',
                'items': {'type': 'string'},
            },
            'allways_request_all_links': {'type': 'boolean'},
            'keep_logs': {
                'oneOf': [
                    {'type': 'integer', 'minimum': 0},
                    {'type': 'string', 'enum': ['all']},
                ],
            },
            'keep_deleted_links': {'type': 'boolean'},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class Config:
    scrapbox: ScrapboxConfig
    git: GitConfig
    external_link: ExternalLinkConfig = ExternalLinkConfig()


def jsonschema_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['scrapbox', 'git'],
        'additional_properties': False,
        'properties': {
            'scrapbox': jsonschema_scrapbox_config(),
            'git': jsonschema_git_config(),
            'external_link': jsonschema_external_link_config(),
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
