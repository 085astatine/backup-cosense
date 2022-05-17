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


@dataclasses.dataclass
class ExternalLinkLog:
    url: str
    locations: list[Location]
    access_timestamp: int
    response: Literal['error'] | ResponseLog
    file_path: Optional[str]


def jsonschema_external_link_log() -> dict[str, Any]:
    schema = {
        'type': 'object',
        'required': [
            'url',
            'locations',
            'access_timestamp',
            'response',
            'file_path',
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
                    {'type': 'string', 'enum': ['error']},
                    jsonschema_response_log(),
                ],
            },
            'file_path': {'type': ['null', 'string']},
        },
    }
    return schema


def jsonschema_external_link_logs() -> dict[str, Any]:
    schema = {
        'type': 'array',
        'items': jsonschema_external_link_log(),
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
    # log directory
    log_directory = pathlib.Path(config.log_directory)
    # save directory
    save_directory = _SaveDirectory(
            root_directory=git_directory,
            links_directory=config.save_directory)
    # load previous log
    previous_log_file = _ExternalLinkLogsFile.find_latest(
            log_directory,
            current=backup.timestamp)
    previous_logs: list[ExternalLinkLog] = []
    if previous_log_file is not None:
        logger.info(
                'load external links from'
                f' "{previous_log_file.path.as_posix()}"')
        previous_logs = previous_log_file.load() or []
    # classify
    classified_links = _classify_external_links(
            backup.external_links(),
            previous_logs)
    # request
    logger.info(f'request {len(classified_links.new_links)} links')
    random.shuffle(classified_links.new_links)
    logs = asyncio.run(_request_external_links(
            classified_links.new_links,
            save_directory,
            config,
            logger))
    # merge logs
    logs.extend(classified_links.logs)
    # sort by URL
    logs.sort(key=lambda log: log.url)
    # save
    save_path = log_directory.joinpath(
            _ExternalLinkLogsFile.filename(backup.timestamp))
    logger.info(f'save external links to "{save_path.as_posix()}"')
    save_json(
            save_path,
            [dataclasses.asdict(log) for log in logs],
            schema=jsonschema_external_link_logs())
    # commit target
    return CommitTarget(
            added=set(
                    git_directory.joinpath(log.file_path) for log in logs
                    if log.file_path is not None),
            deleted=set(
                    git_directory.joinpath(link.file_path)
                    for link in classified_links.deleted_links
                    if link.file_path is not None))


@dataclasses.dataclass
class _ExternalLinkLogsFile:
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
    def filename(cls, timestamp: int) -> str:
        return f'external_link_{timestamp}.json'

    @classmethod
    def find(cls, directory: pathlib.Path) -> list[_ExternalLinkLogsFile]:
        log_files: list[_ExternalLinkLogsFile] = []
        # check if the path is directory
        if not directory.is_dir():
            return log_files
        # find external_link_{timestamp}.json
        for path in directory.iterdir():
            # check if the path is file
            if not path.is_file():
                continue
            # filename match
            if filename_match := re.match(
                    r'external_link_(?P<timestamp>\d+).json',
                    path.name):
                log_files.append(cls(
                        path=path,
                        timestamp=int(filename_match.group('timestamp'))))
        # sort by old...new
        log_files.sort(key=lambda log_file: log_file.timestamp)
        return log_files

    @classmethod
    def find_latest(
            cls,
            directory: pathlib.Path,
            *,
            current: Optional[int] = None) -> Optional[_ExternalLinkLogsFile]:
        return next(
                (log_file for log_file in reversed(cls.find(directory))
                 if current is None or current > log_file.timestamp),
                None)


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
    links_directory: str

    def file_path(self, url: str) -> pathlib.Path:
        return (self.root_directory
                .joinpath(self.links_directory)
                .joinpath(re.sub(r'https?://', '', url)))


async def _request_external_links(
        links: list[ExternalLink],
        save_directory: _SaveDirectory,
        config: ExternalLinkConfig,
        logger: logging.Logger) -> list[ExternalLinkLog]:
    # timeout
    timeout = aiohttp.ClientTimeout(total=config.timeout)
    # semaphore
    semaphore = asyncio.Semaphore(config.parallel_limit)
    # content types
    content_types = [
            re.compile(content_type)
            for content_type in config.content_types]

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
                    logger)
            await asyncio.sleep(config.request_interval)
            return response

    async with aiohttp.ClientSession(
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
        logger: logging.Logger) -> ExternalLinkLog:
    logger.debug(f'request({index}): url={link.url}')
    # access timestamp
    access_timestamp = int(time.time())
    # request
    try:
        async with session.get(link.url) as response:
            logger.debug(f'request({index}): status={response.status}')
            response_log = ResponseLog(
                    status_code=response.status,
                    content_type=response.headers.get('content-type'))
            logger.debug(f'request({index}): response={response_log}')
            file_path: Optional[str] = None
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
                file_path = (
                        save_path
                        .relative_to(save_directory.root_directory)
                        .as_posix())
            return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=access_timestamp,
                response=response_log,
                file_path=file_path)
    except (asyncio.TimeoutError, aiohttp.ClientError) as error:
        logger.debug(f'request({index}): '
                     f'error={error.__class__.__name__}({error})')
        return ExternalLinkLog(
                url=link.url,
                locations=link.locations,
                access_timestamp=access_timestamp,
                response='error',
                file_path=None)
