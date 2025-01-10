import dataclasses
import datetime
import functools
import logging
import operator
import pathlib
from typing import Any, Callable, Literal, Optional, get_args

import dacite
import fake_useragent
import jsonschema
import toml

from ._backup import BackupStorage, PageOrder
from ._git import Git


@dataclasses.dataclass(frozen=True)
class ScrapboxSaveDirectoryConfig:
    name: str
    subdirectory: bool = False

    def storage(self) -> BackupStorage:
        return BackupStorage(
            pathlib.Path(self.name),
            subdirectory=self.subdirectory,
        )


def jsonschema_scrapbox_save_directory_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["name"],
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "subdirectory": {"type": "boolean"},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class ScrapboxConfig:
    project: str
    session_id: str
    save_directory: ScrapboxSaveDirectoryConfig
    request_interval: float = 3.0
    request_timeout: float = 10.0
    backup_start_date: Optional[datetime.datetime] = None


def jsonschema_scrapbox_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["project", "session_id", "save_directory"],
        "additionalProperties": False,
        "properties": {
            "project": {"type": "string"},
            "session_id": {"type": "string"},
            "save_directory": {
                "oneOf": [
                    {"type": "string"},
                    jsonschema_scrapbox_save_directory_config(),
                ],
            },
            "request_interval": {
                "type": "number",
                "exclusiveMinimum": 0.0,
            },
            "request_timeout": {
                "type": "number",
                "exclusiveMinimum": 0.0,
            },
            "backup_start_date": {"type": ["date", "datetime"]},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitEmptyInitialCommitConfig:
    message: str = "Initial commit"
    timestamp: Literal["oldest_backup", "oldest_created_page"] | datetime.datetime = (
        "oldest_created_page"
    )


def jsonschema_git_empty_initial_commit_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "message": {"type": "string"},
            "timestamp": {
                "oneOf": [
                    {
                        "enum": [
                            "oldest_backup",
                            "oldest_created_page",
                        ],
                    },
                    {"type": ["date", "datetime"]},
                ],
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class GitConfig:
    # pylint: disable=too-many-instance-attributes
    path: str
    executable: Optional[str] = None
    branch: Optional[str] = None
    page_order: Optional[PageOrder] = None
    user_name: Optional[str] = None
    user_email: Optional[str] = None
    empty_initial_commit: Optional[GitEmptyInitialCommitConfig] = None
    staging_step_size: int = 1

    def git(self, *, logger: Optional[logging.Logger] = None) -> Git:
        return Git(
            pathlib.Path(self.path),
            executable=self.executable,
            branch=self.branch,
            user_name=self.user_name,
            user_email=self.user_email,
            staging_step_size=self.staging_step_size,
            logger=logger,
        )


def jsonschema_git_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["path"],
        "additionalProperties": False,
        "properties": {
            "path": {"type": "string"},
            "executable": {
                "type": ["string", "null"],
            },
            "branch": {"type": "string"},
            "page_order": {
                "type": ["string", "null"],
                "enum": [None, *get_args(PageOrder)],
            },
            "user_name": {
                "type": ["string", "null"],
            },
            "user_email": {
                "type": ["string", "null"],
            },
            "empty_initial_commit": jsonschema_git_empty_initial_commit_config(),
            "staging_step_size": {
                "type": "integer",
                "minimum": 1,
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class FakeUserAgentConfig:
    os: Optional[Literal["windows", "macos", "linux"]] = None
    browser: Optional[Literal["chrome", "firefox", "safari", "edge"]] = None
    platform: Optional[Literal["pc", "mobile", "tablet"]] = None

    def user_agent(self) -> str:
        generator = fake_useragent.UserAgent(
            os=(self.os if self.os is not None else ["windows", "macos", "linux"]),
            browsers=(
                self.browser
                if self.browser is not None
                else ["chrome", "firefox", "safari", "edge"]
            ),
            platforms=(
                self.platform
                if self.platform is not None
                else ["pc", "mobile", "tablet"]
            ),
        )
        return generator.random


def jsonschema_fake_user_agent_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "os": {
                "type": ["string", "null"],
                "enum": [None, "windows", "macos", "linux"],
            },
            "browser": {
                "type": ["string", "null"],
                "enum": [None, "chrome", "firefox", "safari", "edge"],
            },
            "platform": {
                "type": ["string", "null"],
                "enum": [None, "pc", "mobile", "tablet"],
            },
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class ExternalLinkConfig:
    # pylint: disable=too-many-instance-attributes
    enabled: bool = False
    use_git_lfs: bool = False
    log_directory: str = "log"
    save_directory: str = "links"
    parallel_limit: int = 5
    parallel_limit_per_host: int = 0
    request_interval: float = 1.0
    user_agent: Optional[FakeUserAgentConfig] = None
    request_headers: dict[str, str] = dataclasses.field(default_factory=dict)
    timeout: float = 30.0
    content_types: list[str] = dataclasses.field(default_factory=list)
    excluded_urls: list[str] = dataclasses.field(default_factory=list)
    allways_request_all_links: bool = False
    keep_logs: int | Literal["all"] = "all"
    keep_deleted_links: bool = False


def jsonschema_external_link_config() -> dict[str, Any]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "enabled": {"type": "boolean"},
            "use_git_lfs": {"type": "boolean"},
            "log_directory": {"type": "string"},
            "save_directory": {"type": "string"},
            "parallel_limit": {
                "type": "integer",
                "minimum": 1,
            },
            "parallel_limit_per_host": {
                "type": "integer",
                "minimum": 0,
            },
            "request_interval": {
                "type": "number",
                "exclusiveMinimum": 0.0,
            },
            "user_agent": jsonschema_fake_user_agent_config(),
            "request_headers": {
                "type": "object",
                "additionalProperties": {
                    "type": "string",
                },
            },
            "timeout": {
                "type": "number",
                "exclusiveMinimum": 0.0,
            },
            "content_types": {
                "type": "array",
                "items": {"type": "string"},
            },
            "excluded_urls": {
                "type": "array",
                "items": {"type": "string"},
            },
            "allways_request_all_links": {"type": "boolean"},
            "keep_logs": {
                "oneOf": [
                    {"type": "integer", "minimum": 0},
                    {"type": "string", "enum": ["all"]},
                ],
            },
            "keep_deleted_links": {"type": "boolean"},
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
        "type": "object",
        "required": ["scrapbox", "git"],
        "additional_properties": False,
        "properties": {
            "scrapbox": jsonschema_scrapbox_config(),
            "git": jsonschema_git_config(),
            "external_link": jsonschema_external_link_config(),
        },
    }
    return schema


def load_config(
    path: pathlib.Path,
    *,
    logger: Optional[logging.Logger] = None,
) -> Config:
    logger = logger or logging.getLogger(__name__)
    # load TOML
    logger.info(f'load config from "{path}"')
    with path.open(encoding="utf-8") as file:
        loaded = toml.load(file)
    logger.debug(f"loaded toml: {repr(loaded)}")
    # JSON Schema validation
    _validator().validate(instance=loaded)
    # to dataclass
    _preprocess_to_dataclass(loaded)
    config = dacite.from_dict(
        data_class=Config,
        data=loaded,
        config=dacite.Config(strict=True),
    )
    logger.debug(f"config: {repr(config)}")
    return config


def _validator() -> jsonschema.protocols.Validator:
    Validator = jsonschema.Draft202012Validator
    type_checker = Validator.TYPE_CHECKER.redefine_many(
        {
            "date": _is_date,
            "datetime": _is_datetime,
        }
    )
    return jsonschema.validators.extend(
        Validator,
        type_checker=type_checker,
    )(schema=jsonschema_config())


def _is_date(
    _checker: jsonschema.TypeChecker,
    instance: Any,
) -> bool:
    return isinstance(instance, datetime.date) and not isinstance(
        instance, datetime.datetime
    )


def _is_datetime(
    _checker: jsonschema.TypeChecker,
    instance: Any,
) -> bool:
    return isinstance(instance, datetime.datetime)


def _preprocess_to_dataclass(data: dict) -> None:
    # scrapbox.save_directory
    _update_value(data, ["scrapbox", "save_directory"], _to_save_directory)
    # scrapbox.backup_start_date
    _update_value(data, ["scrapbox", "backup_start_date"], _date_to_datetime)
    # git.empty_initial_commit.timestamp
    _update_value(data, ["git", "empty_initial_commit", "timestamp"], _date_to_datetime)


def _update_value(
    data: dict,
    keys: list[str],
    converter: Callable[[Any], Any],
) -> None:
    if not keys:
        return
    try:
        parent = functools.reduce(operator.getitem, keys[:-1], data)
        parent[keys[-1]] = converter(parent[keys[-1]])
    except KeyError:
        pass


def _to_save_directory(value: str | dict) -> dict:
    if isinstance(value, str):
        return {"name": value}
    return value


def _date_to_datetime(value: datetime.date | datetime.datetime) -> datetime.datetime:
    match value:
        case datetime.datetime():
            return value
        case datetime.date():
            # add time(00:00:00) to date
            return datetime.datetime.combine(value, datetime.time())
