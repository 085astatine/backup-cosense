from __future__ import annotations
import asyncio
import dataclasses
import copy
import logging
import pathlib
import random
import re
import time
from typing import Any, Literal, Optional
import aiohttp
import dacite
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
        'type': 'object',
        'required': ['status_code', 'content_type'],
        'additionalProperties': False,
        'properties': {
            'status_code': {'type': 'integer'},
            'content_type': {'type': ['string', 'null']},
        },
    }
    return schema


@dataclasses.dataclass(frozen=True)
class RequestError:
    error_type: str
    message: str


def jsonschema_request_error() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['error_type', 'message'],
        'additionalProperties': False,
        'properties': {
            'error_type': {'type': 'string'},
            'message': {'type': 'string'},
        },
    }
    return schema


@dataclasses.dataclass
class ExternalLinkLog:
    url: str
    locations: list[Location]
    access_timestamp: int
    response: RequestError | ResponseLog | Literal['excluded']
    is_saved: bool

    @property
    def link(self) -> ExternalLink:
        return ExternalLink(
                url=self.url,
                locations=self.locations)


def jsonschema_external_link_log() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': [
            'url',
            'locations',
            'access_timestamp',
            'response',
            'is_saved',
        ],
        'additionalProperties': False,
        'properties': {
            'url': {'type': 'string'},
            'access_timestamp': {'type': 'integer'},
            'locations': {
                'type': 'array',
                'items': jsonschema_location(),
            },
            'response': {
                'oneOf': [
                    jsonschema_response_log(),
                    jsonschema_request_error(),
                    {'type': 'string', 'enum': ['excluded']},
                ],
            },
            'is_saved': {'type': 'boolean'},
        },
    }
    return schema


def jsonschema_external_link_logs() -> dict[str, Any]:
    schema = {
        'type': 'array',
        'items': jsonschema_external_link_log(),
    }
    return schema


@dataclasses.dataclass(frozen=True)
class SavedExternalLinksInfo:
    content_types: list[str]
    urls: list[str]


def jsonschema_saved_external_links_info() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': ['content_types', 'urls'],
        'additionalProperties': False,
        'properties': {
            'content_types': {
                'type': 'array',
                'items': {'type': 'string'},
            },
            'urls': {
                'type': 'array',
                'items': {'type': 'string'},
            },
        },
    }
    return schema


def save_external_links(
        backup: Backup,
        git_directory: pathlib.Path,
        *,
        config: Optional[ExternalLinkConfig] = None,
        logger: Optional[logging.Logger] = None) -> CommitTarget:
    config = config or ExternalLinkConfig()
    logger = logger or logging.getLogger(__name__)
    request_all = config.allways_request_all_links
    # log directory
    log_directory = _LogsDirectory(
            pathlib.Path(config.log_directory),
            logger)
    # save directory
    save_directory = _SaveDirectory(
            root_directory=git_directory,
            links_directory_name=config.save_directory)
    # load previous saved list
    previous_saved_list = _load_saved_list(save_directory, logger)
    # check previous content_types
    if (previous_saved_list is not None
            and (set(previous_saved_list.content_types)
                 != set(config.content_types))):
        logger.info('request all links because content types are changed')
        request_all = True
    # load previous log
    previous_logs = (
            log_directory.load_latest(timestamp=backup.timestamp) or []
            if not request_all
            else [])
    # check if log file exists
    if (not request_all
            and (logs := log_directory.load(backup.timestamp)) is not None):
        # links
        links = _re_request_targets(
                logs,
                previous_saved_list.urls
                if previous_saved_list is not None else [],
                config.content_types)
        # re-request
        _request_logs(
                links,
                previous_logs,
                save_directory,
                config,
                logger)
    else:
        # request logs
        logs = _request_logs(
                backup.external_links(),
                previous_logs,
                save_directory,
                config,
                logger)
        # save logs
        log_directory.save(backup.timestamp, logs)
    # save list.json
    _save_saved_list(save_directory, config.content_types, logs, logger)
    # commit target
    return _commit_target(
            save_directory,
            logs,
            previous_saved_list,
            config.use_git_lfs,
            logger)


def _request_logs(
        links: list[ExternalLink],
        previous_logs: list[ExternalLinkLog],
        save_directory: _SaveDirectory,
        config: ExternalLinkConfig,
        logger: logging.Logger) -> list[ExternalLinkLog]:
    # classify links
    classified_links = _classify_external_links(
            links,
            previous_logs)
    # shuffle links
    random.shuffle(classified_links.new_links)
    # request
    logs = asyncio.run(_request_external_links(
            classified_links.new_links,
            save_directory,
            config,
            logger))
    # merge previous logs into this logs
    logs.extend(classified_links.logs)
    # sort by URL
    logs.sort(key=lambda log: log.url)
    return logs


@dataclasses.dataclass
class _LogsFile:
    path: pathlib.Path
    timestamp: int

    def load(self) -> Optional[list[ExternalLinkLog]]:
        logs = load_json(
                self.path,
                schema=jsonschema_external_link_logs())
        if logs is None:
            return None
        return [dacite.from_dict(data_class=ExternalLinkLog, data=log)
                for log in logs]

    @classmethod
    def file_name(cls, timestamp: int) -> str:
        return f'external_link_{timestamp}.json'


class _LogsDirectory:
    def __init__(
            self,
            directory: pathlib.Path,
            logger: logging.Logger) -> None:
        self._directory = directory
        self._logger = logger

    def file_path(
            self,
            timestamp: int) -> pathlib.Path:
        return self._directory.joinpath(_LogsFile.file_name(timestamp))

    def find(
            self,
            timestamp: int) -> Optional[_LogsFile]:
        path = self.file_path(timestamp)
        if path.exists():
            return _LogsFile(path=path, timestamp=timestamp)
        return None

    def find_all(self) -> list[_LogsFile]:
        files: list[_LogsFile] = []
        # check if the path is directory
        if not self._directory.is_dir():
            return files
        # find external_link_{timestamp}.json
        for path in self._directory.iterdir():
            # check if the path is file
            if not path.is_file():
                continue
            # filename match
            if filename_match := re.match(
                    r'external_link_(?P<timestamp>\d+).json',
                    path.name):
                files.append(_LogsFile(
                        path=path,
                        timestamp=int(filename_match.group('timestamp'))))
        # sort by old...new
        files.sort(key=lambda file: file.timestamp)
        return files

    def find_latest(
            self,
            *,
            timestamp: Optional[int] = None) -> Optional[_LogsFile]:
        return next(
                (file for file in reversed(self.find_all())
                 if timestamp is None or timestamp > file.timestamp),
                None)

    def load(
            self,
            timestamp: int) -> Optional[list[ExternalLinkLog]]:
        file = self.find(timestamp)
        if file is not None:
            self._logger.info(f'load logs from "{file.path.as_posix()}"')
            logs = file.load()
            if logs is not None:
                return logs
        return None

    def load_latest(
            self,
            *,
            timestamp: Optional[int] = None
    ) -> Optional[list[ExternalLinkLog]]:
        file = self.find_latest(timestamp=timestamp)
        if file is not None:
            self._logger.info(
                    f'load latest logs from "{file.path.as_posix()}"')
            logs = file.load()
            if logs is not None:
                return logs
        return None

    def save(
            self,
            timestamp: int,
            logs: list[ExternalLinkLog]) -> None:
        path = self.file_path(timestamp)
        self._logger.info(f'save logs to "{path.as_posix()}"')
        save_json(
                path,
                [dataclasses.asdict(log) for log in logs],
                schema=jsonschema_external_link_logs())


@dataclasses.dataclass
class _LinkLogPair:
    link: Optional[ExternalLink] = None
    log: Optional[ExternalLinkLog] = None


@dataclasses.dataclass
class _ClassifiedExternalLinks:
    new_links: list[ExternalLink]
    logs: list[ExternalLinkLog]
    deleted_links: list[ExternalLinkLog]


def _classify_external_links(
        links: list[ExternalLink],
        previous_logs: list[ExternalLinkLog]) -> _ClassifiedExternalLinks:
    # link & log pair
    pairs: dict[str, _LinkLogPair] = dict(
            (link.url, _LinkLogPair(link=link)) for link in links)
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
            deleted_links)


@dataclasses.dataclass
class _SaveDirectory:
    root_directory: pathlib.Path
    links_directory: pathlib.Path = dataclasses.field(init=False)
    links_directory_name: dataclasses.InitVar[str]

    def __post_init__(self, links_directory_name: str) -> None:
        self.links_directory = self.root_directory.joinpath(
                links_directory_name)

    def file_path(self, url: str) -> pathlib.Path:
        return self.links_directory.joinpath(re.sub(r'https?://', '', url))

    def list_path(self) -> pathlib.Path:
        return self.links_directory.joinpath('list.json')

    def gitattributes_path(self) -> pathlib.Path:
        return self.links_directory.joinpath('.gitattributes')


async def _request_external_links(
        links: list[ExternalLink],
        save_directory: _SaveDirectory,
        config: ExternalLinkConfig,
        logger: logging.Logger) -> list[ExternalLinkLog]:
    # connector
    connector = aiohttp.TCPConnector(
            limit_per_host=config.parallel_limit_per_host)
    # timeout
    timeout = aiohttp.ClientTimeout(total=config.timeout)
    # semaphore
    semaphore = asyncio.Semaphore(config.parallel_limit)
    # content types
    content_types = [
            re.compile(content_type)
            for content_type in config.content_types]
    # excluded urls
    excluded_urls = [re.compile(url) for url in config.excluded_urls]

    # parallel requests
    async def _parallel_request(
            session: aiohttp.ClientSession,
            index: int,
            link: ExternalLink) -> ExternalLinkLog:
        async with semaphore:
            response = await _request(
                    session,
                    index,
                    link,
                    save_directory,
                    content_types,
                    excluded_urls,
                    logger)
            await asyncio.sleep(config.request_interval)
            return response

    logger.info(f'request {len(links)} links')
    async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=config.request_headers) as session:
        tasks = [
                _parallel_request(session, i, link)
                for i, link in enumerate(links)]
        return await asyncio.gather(*tasks)


async def _request(
        session: aiohttp.ClientSession,
        index: int,
        link: ExternalLink,
        save_directory: _SaveDirectory,
        content_types: list[re.Pattern[str]],
        excluded_urls: list[re.Pattern[str]],
        logger: logging.Logger) -> ExternalLinkLog:
    logger.debug(f'request({index}): url={link.url}')
    # access timestamp
    access_timestamp = int(time.time())
    # check if the url is excluded
    if any(url.match(link.url) is not None for url in excluded_urls):
        logger.debug(f'request({index}): excluded url')
        return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=0,
                response='excluded',
                is_saved=False)
    # request
    try:
        async with session.get(link.url) as response:
            logger.debug(f'request({index}): status={response.status}')
            response_log = ResponseLog(
                    status_code=response.status,
                    content_type=response.headers.get('content-type'))
            logger.debug(f'request({index}): response={response_log}')
            is_saved = False
            # check content type
            if (response_log.content_type is not None
                    and any(content_type.match(response_log.content_type)
                            for content_type in content_types)):
                logger.debug(
                        f'request({index}):'
                        f' save content ({response_log.content_type})')
                # save
                save_path = save_directory.file_path(link.url)
                logger.debug(
                        f'request({index}):'
                        f' save to "{save_path.as_posix()}"')
                if not save_path.parent.exists():
                    save_path.parent.mkdir(parents=True)
                with save_path.open(mode='bw') as file:
                    file.write(await response.read())
                is_saved = True
            return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=access_timestamp,
                response=response_log,
                is_saved=is_saved)
    except (asyncio.TimeoutError, aiohttp.ClientError) as error:
        logger.debug(f'request({index}): '
                     f'error={error.__class__.__name__}({error})')
        return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=access_timestamp,
                response=RequestError(
                        error_type=error.__class__.__name__,
                        message=str(error)),
                is_saved=False)


def _re_request_targets(
        logs: list[ExternalLinkLog],
        saved_urls: list[str],
        content_types: list[str]) -> list[ExternalLink]:
    links: list[ExternalLink] = []
    content_type_patterns = [
            re.compile(content_type) for content_type in content_types]
    for log in logs:
        # check the urls is already saved
        if log.url in saved_urls:
            continue
        match log.response:
            case ResponseLog(content_type=content_type):
                if (content_type is not None
                        and any(pattern.match(content_type)
                                for pattern in content_type_patterns)):
                    links.append(log.link)
    return links


def _load_saved_list(
        directory: _SaveDirectory,
        logger: logging.Logger) -> Optional[SavedExternalLinksInfo]:
    file_path = directory.list_path()
    logger.debug(f'load saved link list from {file_path.as_posix()}')
    data = load_json(
            file_path,
            schema=jsonschema_saved_external_links_info())
    if data is not None:
        return dacite.from_dict(data_class=SavedExternalLinksInfo, data=data)
    return None


def _save_saved_list(
        directory: _SaveDirectory,
        content_types: list[str],
        logs: list[ExternalLinkLog],
        logger: logging.Logger) -> None:
    file_path = directory.list_path()
    saved_list = SavedExternalLinksInfo(
            content_types=sorted(content_types),
            urls=sorted(log.url for log in logs if log.is_saved))
    logger.debug(f'save saved link list to {file_path.as_posix()}')
    save_json(
            file_path,
            dataclasses.asdict(saved_list),
            schema=jsonschema_saved_external_links_info())


def _commit_target(
        directory: _SaveDirectory,
        logs: list[ExternalLinkLog],
        previous_list: Optional[SavedExternalLinksInfo],
        use_git_lfs: bool,
        logger: logging.Logger) -> CommitTarget:
    # saved files
    saved_files = set(
            directory.file_path(log.url) for log in logs
            if log.is_saved)
    previous_saved_files: set[pathlib.Path] = set()
    if previous_list is not None:
        previous_saved_files.update(
                directory.file_path(url) for url in previous_list.urls)
    # added / updated / deleted
    added = saved_files - previous_saved_files
    updated = saved_files & previous_saved_files
    deleted = previous_saved_files - saved_files
    # list.json
    list_path = directory.list_path()
    if list_path.exists():
        updated.add(list_path)
    else:
        added.add(list_path)
    # .gitattributes
    if use_git_lfs:
        gitattributes_path = directory.gitattributes_path()
        if not gitattributes_path.exists():
            write_gitattributes(gitattributes_path)
            added.add(gitattributes_path)
    # remove deleted links
    for path in deleted:
        path.unlink()
    # remove empty directory
    _remove_empty_directory(directory.links_directory, logger)
    return CommitTarget(
            added=added,
            updated=updated,
            deleted=deleted)


def _remove_empty_directory(
        directory: pathlib.Path,
        logger: logging.Logger) -> None:
    if directory.is_dir():
        # recursive
        for child in directory.iterdir():
            _remove_empty_directory(child, logger)
        # check if empty
        if not list(directory.iterdir()):
            logger.debug(f'delete empty directory: "{directory}"')
            directory.rmdir()


def write_gitattributes(path: pathlib.Path) -> None:
    with path.open(mode='w', encoding='utf-8') as file:
        file.write('**/* filter=lfs diff=lfs merge=lfs -text\n')
        file.write('.gitattributes !filter !diff !merge text\n')
        file.write('list.json !filter !diff !merge text\n')
