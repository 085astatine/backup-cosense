import dataclasses
import datetime
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
    request_timeout: float = 10.0


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
            'request_timeout': {
                'type': 'number',
                'exclusiveMinimum': 0.0,
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitEmptyInitialCommitConfig:
    message: str = 'Initial commit'
    timestamp: (
        Literal['oldest_backup', 'oldest_created_page']
        | datetime.date
        | datetime.datetime
    ) = 'oldest_created_page'


def jsonschema_git_empty_initial_commit_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'message': {'type': 'string'},
            'timestamp': {
                'oneOf': [
                    {
                        'enum': [
                            'oldest_backup',
                            'oldest_created_page',
                        ],
                    },
                    {'type': ['date', 'datetime']},
                ],
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitConfig:
    path: str
    executable: Optional[str] = None
    branch: Optional[str] = None
    page_order: Optional[PageOrder] = None
    user_name: Optional[str] = None
    user_email: Optional[str] = None
    empty_initial_commit: Optional[GitEmptyInitialCommitConfig] = None

    def git(self,
            *,
            logger: Optional[logging.Logger] = None) -> Git:
        return Git(
                pathlib.Path(self.path),
                executable=self.executable,
                branch=self.branch,
                user_name=self.user_name,
                user_email=self.user_email,
                logger=logger)


def jsonschema_git_config() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['path'],
        'additionalProperties': False,
        'properties': {
            'path': {'type': 'string'},
            'executable': {
                'type': ['string', 'null'],
            },
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
            'empty_initial_commit':
                jsonschema_git_empty_initial_commit_config(),
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
    _validator().validate(instance=loaded)
    config = dacite.from_dict(
            data_class=Config,
            data=loaded,
            config=dacite.Config(strict=True))
    logger.debug(f'config: {repr(config)}')
    return config


def _validator() -> jsonschema.protocols.Validator:
    Validator = jsonschema.Draft202012Validator
    type_checker = Validator.TYPE_CHECKER.redefine_many({
            'date': _is_date,
            'datetime': _is_datetime})
    return jsonschema.validators.extend(
        Validator,
        type_checker=type_checker)(schema=jsonschema_config())


def _is_date(
        _checker: jsonschema.TypeChecker,
        instance: Any) -> bool:
    return isinstance(instance, datetime.date)


def _is_datetime(
        _checker: jsonschema.TypeChecker,
        instance: Any) -> bool:
    return isinstance(instance, datetime.datetime)
