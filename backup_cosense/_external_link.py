from __future__ import annotations

import asyncio
import copy
import dataclasses
import logging
import pathlib
import random
import re
import time
from collections import defaultdict
from typing import Any, Literal, Optional, Self

import aiohttp
import dacite
import multidict

from ._backup import Backup, ExternalLink, Location, jsonschema_location
from ._config import ExternalLinkConfig
from ._git import CommitTarget
from ._json import load_json, save_json


@dataclasses.dataclass
class ResponseLog:
    status_code: int
    content_type: Optional[str]


def jsonschema_response_log() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["status_code", "content_type"],
        "additionalProperties": False,
        "properties": {
            "status_code": {"type": "integer"},
            "content_type": {"type": ["string", "null"]},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class RequestError:
    error_type: str
    message: str


def jsonschema_request_error() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["error_type", "message"],
        "additionalProperties": False,
        "properties": {
            "error_type": {"type": "string"},
            "message": {"type": "string"},
        },
    }
    return schema


@dataclasses.dataclass
class ExternalLinkLog:
    url: str
    locations: list[Location]
    access_timestamp: int
    response: RequestError | ResponseLog | Literal["excluded"]
    is_saved: bool

    @property
    def link(self) -> ExternalLink:
        return ExternalLink(url=self.url, locations=self.locations)


def jsonschema_external_link_log() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": [
            "url",
            "locations",
            "access_timestamp",
            "response",
            "is_saved",
        ],
        "additionalProperties": False,
        "properties": {
            "url": {"type": "string"},
            "access_timestamp": {"type": "integer"},
            "locations": {
                "type": "array",
                "items": jsonschema_location(),
            },
            "response": {
                "oneOf": [
                    jsonschema_response_log(),
                    jsonschema_request_error(),
                    {"const": "excluded"},
                ],
            },
            "is_saved": {"type": "boolean"},
        },
    }
    return schema


def jsonschema_external_link_logs() -> dict[str, Any]:
    schema = {
        "type": "array",
        "items": jsonschema_external_link_log(),
    }
    return schema


@dataclasses.dataclass(frozen=True)
class SavedExternalLinksInfo:
    content_types: list[str]
    urls: list[str]


def jsonschema_saved_external_links_info() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["content_types", "urls"],
        "additionalProperties": False,
        "properties": {
            "content_types": {
                "type": "array",
                "items": {"type": "string"},
            },
            "urls": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }
    return schema


def save_external_links(
    backup: Backup,
    git_directory: pathlib.Path,
    *,
    config: Optional[ExternalLinkConfig] = None,
    session: Optional[aiohttp.ClientSession] = None,
    logger: Optional[logging.Logger] = None,
) -> CommitTarget:
    config = config or ExternalLinkConfig()
    logger = logger or logging.getLogger(__name__)
    # session
    if session is None:
        session = _create_session(config)
    # log directory
    log_directory = _LogDirectory(pathlib.Path(config.log_directory), logger)
    # links directory
    links_directory = _LinksDirectory(
        git_directory.joinpath(config.save_directory),
        logger,
    )
    # load previous saved list
    previous_saved_list = links_directory.load_file_list()
    # load previous log
    previous_logs = (
        log_directory.load_latest(timestamp=backup.timestamp) or []
        if not config.allways_request_all_links
        else []
    )
    # check if log file exists
    if (
        not config.allways_request_all_links
        and (logs := log_directory.load(backup.timestamp)) is not None
    ):
        # links
        links = _re_request_targets(
            logs,
            previous_saved_list.urls if previous_saved_list is not None else [],
            config.content_types,
        )
        # re-request
        _request_logs(
            links,
            previous_logs,
            links_directory,
            config,
            session,
            logger,
        )
    else:
        # request logs
        logs = _request_logs(
            backup.external_links(),
            previous_logs,
            links_directory,
            config,
            session,
            logger,
        )
        # save logs
        log_directory.save(backup.timestamp, logs)
    # save list.json
    links_directory.save_file_list(
        config.content_types,
        [log.url for log in logs if log.is_saved],
    )
    # clean logs
    if config.keep_logs != "all":
        log_directory.clean(config.keep_logs)
    # commit target
    return _commit_target(
        config,
        links_directory,
        logs,
        previous_saved_list,
    )


def _create_session(config: ExternalLinkConfig) -> aiohttp.ClientSession:
    # connector
    connector = aiohttp.TCPConnector(limit_per_host=config.parallel_limit_per_host)
    # timeout
    timeout = aiohttp.ClientTimeout(total=config.timeout)
    # To fix ClientResponseError
    #  Got more than 8190 bytes (xxxx) when reading Header value is too long
    max_size = 8190 * 2
    return aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=_request_headers(config),
        max_line_size=max_size,
        max_field_size=max_size,
    )


def _request_logs(
    # pylint: disable=too-many-arguments, too-many-positional-arguments
    links: list[ExternalLink],
    previous_logs: list[ExternalLinkLog],
    links_directory: _LinksDirectory,
    config: ExternalLinkConfig,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
) -> list[ExternalLinkLog]:
    # classify links
    classified_links = _classify_external_links(
        links,
        previous_logs,
    )
    # shuffle links
    random.shuffle(classified_links.new_links)
    # request
    logs = asyncio.run(
        _request_external_links(
            classified_links.new_links,
            links_directory,
            config,
            session,
            logger,
        )
    )
    # merge previous logs into this logs
    logs.extend(classified_links.logs)
    # sort by URL
    logs.sort(key=lambda log: log.url)
    return logs


@dataclasses.dataclass
class _LogFile:
    path: pathlib.Path
    timestamp: int


@dataclasses.dataclass
class _LinkLogPair:
    link: Optional[ExternalLink] = None
    log: Optional[ExternalLinkLog] = None


@dataclasses.dataclass
class _Log:
    timestamp: int
    logs: list[ExternalLinkLog]
    added_links: list[ExternalLink] = dataclasses.field(default_factory=list)
    deleted_links: list[str] = dataclasses.field(default_factory=list)

    def update_links(
        self,
        timestamp: int,
        links: list[ExternalLink],
    ) -> Self:
        # match link & log
        pairs: dict[str, _LinkLogPair] = defaultdict(_LinkLogPair)
        for link in links:
            pairs[link.url].link = link
        for log in self.logs:
            pairs[log.url].log = log
        # classify
        logs: list[ExternalLinkLog] = []
        added_links: list[ExternalLink] = []
        deleted_links: list[str] = []
        for pair in pairs.values():
            if pair.log is None:
                if pair.link is None:
                    pass
                else:
                    added_links.append(pair.link)
            else:
                if pair.link is None:
                    deleted_links.append(pair.log.url)
                else:
                    # replace locations
                    log = copy.copy(pair.log)
                    log.locations = copy.copy(pair.link.locations)
                    logs.append(log)
        return self.__class__(
            timestamp=timestamp,
            logs=logs,
            added_links=added_links,
            deleted_links=deleted_links,
        )

    @classmethod
    def load(cls, path: pathlib.Path, timestamp: int) -> Optional[Self]:
        logs = load_json(path, schema=jsonschema_external_link_logs())
        if logs is None:
            return None
        return cls(
            timestamp=timestamp,
            logs=[
                dacite.from_dict(data_class=ExternalLinkLog, data=log) for log in logs
            ],
        )

    def save(self, path: pathlib.Path) -> None:
        # sort by URL
        logs = sorted(self.logs, key=lambda log: log.url)
        save_json(
            path,
            logs,
            schema=jsonschema_external_link_log(),
        )


class _LogDirectory:
    def __init__(
        self,
        directory: pathlib.Path,
        logger: logging.Logger,
    ) -> None:
        self._directory = directory
        self._logger = logger

    def file_path(self, timestamp: int) -> pathlib.Path:
        return self._directory.joinpath(f"external_link_{timestamp}.json")

    def find(self, timestamp: int) -> Optional[_LogFile]:
        path = self.file_path(timestamp)
        if path.exists():
            return _LogFile(path=path, timestamp=timestamp)
        return None

    def find_all(self) -> list[_LogFile]:
        # check if the path is directory
        if not self._directory.is_dir():
            return []
        # find external_link_{timestamp}.json
        files: list[_LogFile] = []
        pattern = re.compile(r"external_link_(?P<timestamp>[0-9]+).json")
        for path in self._directory.iterdir():
            # check if the path is file
            if not path.is_file():
                continue
            # filename match
            if match := pattern.match(path.name):
                files.append(
                    _LogFile(path=path, timestamp=int(match.group("timestamp")))
                )
        # sort by old...new
        files.sort(key=lambda file: file.timestamp)
        return files

    def find_latest(
        self,
        *,
        timestamp: Optional[int] = None,
    ) -> Optional[_LogFile]:
        return next(
            (
                file
                for file in reversed(self.find_all())
                if timestamp is None or timestamp > file.timestamp
            ),
            None,
        )

    def load(self, timestamp: int) -> Optional[list[ExternalLinkLog]]:
        file = self.find(timestamp)
        if file is not None:
            self._logger.info(f'load log from "{file.path}"')
            log = _Log.load(file.path, file.timestamp)
            if log is not None:
                return log.logs
        return None

    def load_latest(
        self,
        *,
        timestamp: Optional[int] = None,
    ) -> Optional[list[ExternalLinkLog]]:
        file = self.find_latest(timestamp=timestamp)
        if file is not None:
            self._logger.info(f'load latest log from "{file.path}"')
            log = _Log.load(file.path, file.timestamp)
            if log is not None:
                return log.logs
        return None

    def save(
        self,
        timestamp: int,
        logs: list[ExternalLinkLog],
    ) -> None:
        path = self.file_path(timestamp)
        self._logger.info(f'save logs to "{path}"')
        save_json(
            path,
            [dataclasses.asdict(log) for log in logs],
            schema=jsonschema_external_link_logs(),
        )

    def clean(self, keep: int) -> None:
        if keep >= 0:
            targets = list(reversed(self.find_all()))[keep:]
            self._logger.info(f"clean {len(targets)} log files")
            for target in targets:
                self._logger.info(f"delete log file: {target.path}")
                target.path.unlink()
        else:
            self._logger.warning(f"skip clean: keep({keep}) must be >= 0")


@dataclasses.dataclass
class _ClassifiedExternalLinks:
    new_links: list[ExternalLink]
    logs: list[ExternalLinkLog]
    deleted_links: list[ExternalLinkLog]


def _classify_external_links(
    links: list[ExternalLink],
    previous_logs: list[ExternalLinkLog],
) -> _ClassifiedExternalLinks:
    # link & log pair
    pairs = {link.url: _LinkLogPair(link=link) for link in links}
    for log in previous_logs:
        if log.url in pairs:
            pairs[log.url].log = log
        else:
            pairs[log.url] = _LinkLogPair(log=log)
    # classify
    new_links: list[ExternalLink] = []
    logs: list[ExternalLinkLog] = []
    deleted_links: list[ExternalLinkLog] = []
    for pair in pairs.values():
        if pair.log is None:
            if pair.link is not None:
                new_links.append(pair.link)
        else:
            if pair.link is None:
                deleted_links.append(pair.log)
            else:
                # replace locations
                log = copy.copy(pair.log)
                log.locations = copy.copy(pair.link.locations)
                logs.append(log)
    return _ClassifiedExternalLinks(
        new_links,
        logs,
        deleted_links,
    )


class _LinksDirectory:
    def __init__(
        self,
        path: pathlib.Path,
        logger: logging.Logger,
    ) -> None:
        self._path = path
        self._logger = logger

    def file_path(self, url: str) -> pathlib.Path:
        return self._path.joinpath(re.sub(r"https?://", "", url))

    def file_list_path(self) -> pathlib.Path:
        return self._path.joinpath("list.json")

    def load_file_list(self) -> Optional[SavedExternalLinksInfo]:
        path = self.file_list_path()
        self._logger.debug(f"load saved link list from {path}")
        data = load_json(path, schema=jsonschema_saved_external_links_info())
        if data is not None:
            return dacite.from_dict(data_class=SavedExternalLinksInfo, data=data)
        return None

    def save_file_list(
        self,
        content_types: list[str],
        urls: list[str],
    ) -> None:
        path = self.file_list_path()
        self._logger.debug(f"save saved link list to {path}")
        data = {
            "content_types": sorted(content_types),
            "urls": sorted(urls),
        }
        save_json(path, data, schema=jsonschema_saved_external_links_info())

    def gitattributes_path(self) -> pathlib.Path:
        return self._path.joinpath(".gitattributes")

    def create_gitattributes(self) -> None:
        with self.gitattributes_path().open(mode="w", encoding="utf-8") as file:
            file.write("**/* filter=lfs diff=lfs merge=lfs -text\n")
            file.write(".gitattributes !filter !diff !merge text\n")
            file.write("list.json !filter !diff !merge text\n")

    def remove_empty_directory(self) -> None:
        # execute recursively
        def _remove_empty_directory(path: pathlib.Path) -> None:
            # check if the path is directory
            if not path.is_dir():
                return
            # execute recursively to child directories
            for child in path.iterdir():
                _remove_empty_directory(child)
            # check if empty
            if not list(path.iterdir()):
                self._logger.debug(f'delete empty directory: "{path}"')
                path.rmdir()

        # execute from the root directory
        _remove_empty_directory(self._path)


async def _request_external_links(
    links: list[ExternalLink],
    links_directory: _LinksDirectory,
    config: ExternalLinkConfig,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
) -> list[ExternalLinkLog]:
    # semaphore
    semaphore = asyncio.Semaphore(config.parallel_limit)
    # request config
    request_config = _RequestConfig(
        links_directory=links_directory,
        content_types=[
            re.compile(content_type) for content_type in config.content_types
        ],
        excluded_urls=[re.compile(url) for url in config.excluded_urls],
    )

    # parallel requests
    async def _parallel_request(
        session: aiohttp.ClientSession,
        index: int,
        link: ExternalLink,
    ) -> ExternalLinkLog:
        async with semaphore:
            response = await _request(
                session,
                index,
                link,
                request_config,
                logger,
            )
            await asyncio.sleep(config.request_interval)
            return response

    logger.info(f"request {len(links)} links")

    async with session:
        tasks = [_parallel_request(session, i, link) for i, link in enumerate(links)]
        return await asyncio.gather(*tasks)


@dataclasses.dataclass
class _RequestConfig:
    links_directory: _LinksDirectory
    content_types: list[re.Pattern[str]]
    excluded_urls: list[re.Pattern[str]]


async def _request(
    session: aiohttp.ClientSession,
    index: int,
    link: ExternalLink,
    config: _RequestConfig,
    logger: logging.Logger,
) -> ExternalLinkLog:
    logger.debug(f"request({index}): url={link.url}")
    # access timestamp
    access_timestamp = int(time.time())
    # check if the url is excluded
    if any(url.match(link.url) is not None for url in config.excluded_urls):
        logger.debug(f"request({index}): excluded url")
        return ExternalLinkLog(
            url=link.url,
            locations=link.locations,
            access_timestamp=0,
            response="excluded",
            is_saved=False,
        )
    # request
    try:
        async with session.get(link.url) as response:
            logger.debug(f"request({index}): status={response.status}")
            response_log = ResponseLog(
                status_code=response.status,
                content_type=response.headers.get("content-type"),
            )
            logger.debug(f"request({index}): response={response_log}")
            is_saved = False
            # check content type
            if response_log.content_type is not None and any(
                content_type.match(response_log.content_type)
                for content_type in config.content_types
            ):
                logger.debug(
                    f"request({index}): save content ({response_log.content_type})"
                )
                # save
                file_path = config.links_directory.file_path(link.url)
                logger.debug(f'request({index}): save to "{file_path}"')
                if not file_path.parent.exists():
                    file_path.parent.mkdir(parents=True)
                with file_path.open(mode="bw") as file:
                    file.write(await response.read())
                is_saved = True
            return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=access_timestamp,
                response=response_log,
                is_saved=is_saved,
            )
    except (asyncio.TimeoutError, aiohttp.ClientError) as error:
        logger.debug(f"request({index}): error={error.__class__.__name__}({error})")
        return ExternalLinkLog(
            url=link.url,
            locations=link.locations,
            access_timestamp=access_timestamp,
            response=RequestError(
                error_type=error.__class__.__name__,
                message=str(error),
            ),
            is_saved=False,
        )


def _request_headers(config: ExternalLinkConfig) -> multidict.CIMultiDict:
    headers: multidict.CIMultiDict = multidict.CIMultiDict()
    # config.user_agent
    if config.user_agent is not None:
        headers["User-Agent"] = config.user_agent.user_agent()
    # config.request_headers
    for key, value in config.request_headers.items():
        headers[key] = value
    return headers


def _re_request_targets(
    logs: list[ExternalLinkLog],
    saved_urls: list[str],
    content_types: list[str],
) -> list[ExternalLink]:
    links: list[ExternalLink] = []
    content_type_patterns = [re.compile(content_type) for content_type in content_types]
    for log in logs:
        # check the urls is already saved
        if log.url in saved_urls:
            continue
        match log.response:
            case ResponseLog(content_type=content_type):
                if content_type is not None and any(
                    pattern.match(content_type) for pattern in content_type_patterns
                ):
                    links.append(log.link)
    return links


def _commit_target(
    config: ExternalLinkConfig,
    directory: _LinksDirectory,
    logs: list[ExternalLinkLog],
    previous_list: Optional[SavedExternalLinksInfo],
) -> CommitTarget:
    # saved files
    saved_files = {directory.file_path(log.url) for log in logs if log.is_saved}
    previous_saved_files: set[pathlib.Path] = set()
    if previous_list is not None:
        previous_saved_files.update(
            directory.file_path(url) for url in previous_list.urls
        )
    # added / updated / deleted
    added = saved_files - previous_saved_files
    updated = saved_files & previous_saved_files
    deleted: set[pathlib.Path] = set()
    if not config.keep_deleted_links:
        deleted.update(previous_saved_files - saved_files)
    # list.json
    list_path = directory.file_list_path()
    if list_path.exists():
        updated.add(list_path)
    else:
        added.add(list_path)
    # .gitattributes
    if config.use_git_lfs:
        gitattributes_path = directory.gitattributes_path()
        if not gitattributes_path.exists():
            directory.create_gitattributes()
            added.add(gitattributes_path)
    # remove deleted links
    for path in deleted:
        path.unlink()
    # remove empty directory
    directory.remove_empty_directory()
    return CommitTarget(added=added, updated=updated, deleted=deleted)
