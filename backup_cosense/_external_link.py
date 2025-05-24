from __future__ import annotations

import asyncio
import dataclasses
import logging
import pathlib
import random
import re
import time
from typing import Any, Callable, Literal, MutableMapping, Optional, Self

import aiohttp
import dacite
import multidict

from ._backup import ExternalLink, Location
from ._config import ExternalLinkConfig, ExternalLinkSessionConfig
from ._git import CommitTarget
from ._json import load_json, save_json


@dataclasses.dataclass(frozen=True)
class ResponseLog:
    status_code: int
    content_type: Optional[str]

    @classmethod
    def jsonschema(cls) -> dict[str, Any]:
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

    @classmethod
    def jsonschema(cls) -> dict[str, Any]:
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


@dataclasses.dataclass(frozen=True)
class ExternalLinkLog:
    url: str
    locations: list[Location]
    access_timestamp: int
    response: RequestError | ResponseLog | Literal["excluded"]
    is_saved: bool

    @property
    def link(self) -> ExternalLink:
        return ExternalLink(url=self.url, locations=self.locations)

    @classmethod
    def jsonschema(cls) -> dict[str, Any]:
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
                    "items": Location.jsonschema(),
                },
                "response": {
                    "oneOf": [
                        ResponseLog.jsonschema(),
                        RequestError.jsonschema(),
                        {"const": "excluded"},
                    ],
                },
                "is_saved": {"type": "boolean"},
            },
        }
        return schema


def save_external_links(
    # pylint: disable=too-many-arguments
    timestamp: int,
    external_links: list[ExternalLink],
    git_directory: pathlib.Path,
    *,
    config: Optional[ExternalLinkConfig] = None,
    create_session: Optional[Callable[[], aiohttp.ClientSession]] = None,
    logger: Optional[logging.Logger] = None,
) -> CommitTarget:
    config = config or ExternalLinkConfig()
    logger = logger or logging.getLogger(__name__)

    # session
    def default_create_session() -> aiohttp.ClientSession:
        return _create_session(config.session)

    if create_session is None:
        create_session = default_create_session
    # log directory
    log_directory = _LogDirectory(pathlib.Path(config.log_directory), logger)
    # links directory
    links_directory = _LinksDirectory(
        git_directory.joinpath(config.save_directory),
        logger,
    )
    # files saved before request
    already_saved_files = set(links_directory.files())
    # log editor
    log_editor = _setup_log_editor(
        timestamp,
        external_links,
        log_directory,
        config.allways_request_all_links,
        logger,
    )
    # request
    _request(
        log_editor,
        links_directory,
        config,
        create_session,
        logger,
    )
    # save log
    log = log_editor.output()
    log_directory.save(log)
    # clean logs
    if config.keep_logs != "all":
        log_directory.clean(config.keep_logs)
    # commit target
    return _commit_target(
        config,
        external_links,
        links_directory,
        log_editor,
        already_saved_files,
    )


def _create_session(config: ExternalLinkSessionConfig) -> aiohttp.ClientSession:
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


def _request_headers(config: ExternalLinkSessionConfig) -> multidict.CIMultiDict:
    headers: multidict.CIMultiDict = multidict.CIMultiDict()
    # config.user_agent
    if config.user_agent is not None:
        headers["User-Agent"] = config.user_agent.create()
    # config.request_headers
    for key, value in config.request_headers.items():
        headers[key] = value
    return headers


@dataclasses.dataclass(frozen=True)
class _LogFile:
    path: pathlib.Path
    timestamp: int


@dataclasses.dataclass(frozen=True)
class _Log:
    timestamp: int
    logs: list[ExternalLinkLog]

    @classmethod
    def load(cls, path: pathlib.Path, timestamp: int) -> Optional[Self]:
        logs = load_json(path, schema=cls.jsonschema())
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
            [dataclasses.asdict(log) for log in logs],
            schema=self.jsonschema(),
        )

    @classmethod
    def jsonschema(cls) -> dict[str, Any]:
        schema = {
            "type": "array",
            "items": ExternalLinkLog.jsonschema(),
        }
        return schema


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

    def load(self, timestamp: int) -> Optional[_Log]:
        file = self.find(timestamp)
        if file is not None:
            self._logger.info(f'load log from "{file.path}"')
            return _Log.load(file.path, file.timestamp)
        return None

    def load_latest(
        self,
        *,
        timestamp: Optional[int] = None,
    ) -> Optional[_Log]:
        file = self.find_latest(timestamp=timestamp)
        if file is not None:
            self._logger.info(f'load latest log from "{file.path}"')
            return _Log.load(file.path, file.timestamp)
        return None

    def save(self, log: _Log) -> None:
        path = self.file_path(log.timestamp)
        self._logger.info(f'save logs to "{path}"')
        log.save(path)

    def clean(self, keep: int) -> None:
        if keep >= 0:
            targets = list(reversed(self.find_all()))[keep:]
            self._logger.info(f"clean {len(targets)} log files")
            for target in targets:
                self._logger.info(f"delete log file: {target.path}")
                target.path.unlink()
        else:
            self._logger.warning(f"skip clean: keep({keep}) must be >= 0")


class _LogEditor:
    def __init__(
        self,
        timestamp: int,
        logger: logging.Logger,
    ) -> None:
        self._timestamp = timestamp
        self._logger = logger
        self._logs: dict[str, ExternalLinkLog] = {}
        self._added_links: dict[str, ExternalLink] = {}
        self._updated_logs: dict[str, ExternalLinkLog] = {}
        self._deleted_links: set[str] = set()

    def load_logs(self, logs: list[ExternalLinkLog]) -> None:
        self._logs.update({log.url: log for log in logs})

    def update_links(self, links: list[ExternalLink]) -> None:
        # match link & log
        logs: dict[str, ExternalLinkLog] = {}
        added_links: dict[str, ExternalLink] = {}
        for link in links:
            if link.url in self._logs:
                # update locations
                log = self._logs[link.url]
                logs[log.url] = ExternalLinkLog(
                    url=log.url,
                    locations=link.locations[:],
                    access_timestamp=log.access_timestamp,
                    response=log.response,
                    is_saved=log.is_saved,
                )
            else:
                added_links[link.url] = link
        deleted_links = set(self._logs.keys())
        # update
        self._logs = logs
        self._added_links = added_links
        self._deleted_links = deleted_links

    def update_log(self, log: ExternalLinkLog) -> None:
        # delete from added links
        self._added_links.pop(log.url, None)
        # add to logs
        self._logs[log.url] = log
        self._updated_logs[log.url] = log

    def added_links(self) -> list[ExternalLink]:
        return list(self._added_links.values())

    def logs(self) -> list[ExternalLinkLog]:
        return list(self._logs.values())

    def updated_logs(self) -> list[ExternalLinkLog]:
        return list(self._updated_logs.values())

    def output(self) -> _Log:
        if self._added_links:
            self._logger.warning("no logs exist for {len(self._added_links)} URLs")
        return _Log(
            timestamp=self._timestamp,
            logs=sorted(self._logs.values(), key=lambda log: log.url),
        )


class _LinksDirectory:
    def __init__(
        self,
        path: pathlib.Path,
        logger: logging.Logger,
    ) -> None:
        self._path = path
        self._logger = logger

    @property
    def path(self) -> pathlib.Path:
        return self._path

    def file_path(self, url: str) -> pathlib.Path:
        return self._path.joinpath(_url_to_path(url))

    def files(self) -> list[pathlib.Path]:
        return [path for path in self._path.glob("*/**/*") if path.is_file()]

    def gitattributes_path(self) -> pathlib.Path:
        return self._path.joinpath(".gitattributes")

    def create_gitattributes(self) -> None:
        with self.gitattributes_path().open(mode="w", encoding="utf-8") as file:
            file.write("**/* filter=lfs diff=lfs merge=lfs -text\n")
            file.write(".gitattributes !filter !diff !merge text\n")

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


def _url_to_path(url: str) -> str:
    return re.sub(r"https?://", "", url)


def _setup_log_editor(
    timestamp: int,
    links: list[ExternalLink],
    log_directory: _LogDirectory,
    all_request: bool,
    logger: logging.Logger,
) -> _LogEditor:
    # load previous log
    previous_log = log_directory.load_latest(timestamp=timestamp)
    # setup log editor
    editor = _LogEditor(timestamp, logger)
    if not all_request and previous_log is not None:
        editor.load_logs(previous_log.logs)
    editor.update_links(links)
    return editor


def _request(
    editor: _LogEditor,
    links_directory: _LinksDirectory,
    config: ExternalLinkConfig,
    create_session: Callable[[], aiohttp.ClientSession],
    logger: logging.Logger,
) -> None:
    # shuffle links
    links = editor.added_links()
    random.shuffle(links)
    # request
    logs = asyncio.run(
        _request_external_links(
            links,
            links_directory,
            config,
            create_session,
            logger,
        )
    )
    # add new log
    for log in logs:
        editor.update_log(log)


async def _request_external_links(
    links: list[ExternalLink],
    links_directory: _LinksDirectory,
    config: ExternalLinkConfig,
    create_session: Callable[[], aiohttp.ClientSession],
    logger: logging.Logger,
) -> list[ExternalLinkLog]:
    # semaphore
    semaphore = asyncio.Semaphore(config.parallel_limit)
    # request arguments
    request_args = _RequestArguments(
        links_directory=links_directory,
        content_types=[re.compile(pattern) for pattern in config.content_types],
        excluded_urls=[re.compile(pattern) for pattern in config.excluded_urls],
    )

    # parallel requests
    async def _parallel_request(
        session: aiohttp.ClientSession,
        index: int,
        link: ExternalLink,
    ) -> ExternalLinkLog:
        async with semaphore:
            response = await _request_link(
                request_args,
                session,
                link,
                _RequestLogger(logger, index),
            )
            await asyncio.sleep(config.request_interval)
            return response

    logger.info(f"request {len(links)} links")

    async with create_session() as session:
        tasks = [_parallel_request(session, i, link) for i, link in enumerate(links)]
        return await asyncio.gather(*tasks)


@dataclasses.dataclass(frozen=True)
class _RequestArguments:
    links_directory: _LinksDirectory
    content_types: list[re.Pattern[str]]
    excluded_urls: list[re.Pattern[str]]

    def is_target_content_type(self, content_type: Optional[str]) -> bool:
        if content_type is None:
            return False
        return any(pattern.match(content_type) for pattern in self.content_types)

    def is_excluded_url(self, url: str) -> bool:
        return any(pattern.match(url) for pattern in self.excluded_urls)


class _RequestLogger(logging.LoggerAdapter):
    def __init__(
        self,
        logger: logging.Logger,
        index: int,
    ) -> None:
        super().__init__(logger)
        self._index = index

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        return super().process(f"request({self._index}): {msg}", kwargs)


async def _request_link(
    args: _RequestArguments,
    session: aiohttp.ClientSession,
    link: ExternalLink,
    logger: _RequestLogger,
) -> ExternalLinkLog:
    logger.debug(f"url={link.url}")
    # access timestamp
    access_timestamp = int(time.time())
    # check if the url is excluded
    if args.is_excluded_url(link.url):
        logger.debug("excluded url")
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
            logger.debug(f"status={response.status}")
            response_log = ResponseLog(
                status_code=response.status,
                content_type=response.headers.get("content-type"),
            )
            logger.debug(f"response={response_log}")
            is_saved = False
            # check content type
            if args.is_target_content_type(response_log.content_type):
                logger.debug(f"save content ({response_log.content_type})")
                # save
                file_path = args.links_directory.file_path(link.url)
                logger.debug(f'save to "{file_path}"')
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
        logger.debug(f"error={error.__class__.__name__}({error})")
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


def _commit_target(
    config: ExternalLinkConfig,
    links: list[ExternalLink],
    directory: _LinksDirectory,
    log_editor: _LogEditor,
    already_saved_files: set[pathlib.Path],
) -> CommitTarget:
    # updated files
    updated_files = {
        directory.file_path(log.url)
        for log in log_editor.updated_logs()
        if log.is_saved
    }
    # added / updated / deleted
    added = updated_files - already_saved_files
    updated = updated_files & already_saved_files
    deleted: set[pathlib.Path] = set()
    if not config.keep_deleted_links:
        deleted = _files_with_no_links(directory, links)
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


def _files_with_no_links(
    links_directory: _LinksDirectory,
    external_links: list[ExternalLink],
) -> set[pathlib.Path]:
    saved_files = set(
        path.relative_to(links_directory.path).as_posix()
        for path in links_directory.files()
    )
    linked_files = set(_url_to_path(link.url) for link in external_links)
    no_linked_files = saved_files - linked_files
    return set(links_directory.path.joinpath(path) for path in no_linked_files)
