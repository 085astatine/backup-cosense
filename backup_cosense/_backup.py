from __future__ import annotations

import dataclasses
import itertools
import logging
import math
import pathlib
import re
from typing import Any, Generator, Literal, Optional, Self, Tuple, TypedDict

import jsonschema

from ._json import load_json, save_json

PageOrder = Literal["as-is", "created-asc", "created-desc"]
InternalLinkType = Literal["page", "word"]


class BackupInfoJSON(TypedDict):
    id: str
    backuped: int
    totalPages: int
    totalLinks: int


def jsonschema_backup_info() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["id", "backuped"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string"},
            "backuped": {"type": "integer"},
            "totalPages": {"type": "integer"},
            "totalLinks": {"type": "integer"},
        },
    }
    return schema


class UserJSON(TypedDict):
    id: str
    name: str
    displayName: str
    email: str


def jsonschema_user() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["id", "name", "displayName", "email"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "displayName": {"type": "string"},
            "email": {"type": "string"},
        },
    }
    return schema


class BackupPageLineJSON(TypedDict):
    text: str
    created: int
    updated: int


def jsonschema_backup_page_line() -> dict[str, Any]:
    schema = {
        "type": "object",
        "requred": ["text", "created", "updated"],
        "additionalProperties": False,
        "properties": {
            "text": {"type": "string"},
            "created": {"type": "integer"},
            "updated": {"type": "integer"},
            "userId": {"type": "string"},
        },
    }
    return schema


class BackupPageJSON(TypedDict):
    title: str
    created: int
    updated: int
    id: str
    views: Optional[int]
    lines: list[str] | list[BackupPageLineJSON]
    linksLc: list[str]


def page_lines(page: BackupPageJSON) -> Generator[str, None, None]:
    for line in page["lines"]:
        match line:
            case str():
                yield line
            case {"text": text}:
                yield text


def jsonschema_backup_page() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["title", "created", "updated", "lines"],
        "additionalProperties": False,
        "properties": {
            "title": {"type": "string"},
            "created": {"type": "integer"},
            "updated": {"type": "integer"},
            "id": {"type": "string"},
            "views": {"type": "integer"},
            "lines": {
                "oneOf": [
                    {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    {
                        "type": "array",
                        "items": jsonschema_backup_page_line(),
                    },
                ],
            },
            "linksLc": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }
    return schema


class BackupJSON(TypedDict):
    name: str
    displayName: str
    exported: int
    users: list[UserJSON]
    pages: list[BackupPageJSON]


def jsonschema_backup() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["name", "displayName", "exported", "pages"],
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "displayName": {"type": "string"},
            "exported": {"type": "integer"},
            "users": {
                "type": "array",
                "items": jsonschema_user(),
            },
            "pages": {
                "type": "array",
                "items": jsonschema_backup_page(),
            },
        },
    }
    return schema


@dataclasses.dataclass(order=True, frozen=True)
class Location:
    title: str
    line: int

    @classmethod
    def jsonschema(cls) -> dict[str, Any]:
        schema = {
            "type": "object",
            "required": ["title", "line"],
            "additionalProperties": False,
            "properties": {
                "title": {"type": "string"},
                "line": {"type": "integer"},
            },
        }
        return schema


@dataclasses.dataclass(frozen=True)
class InternalLinkNode:
    name: str
    type: InternalLinkType


@dataclasses.dataclass(frozen=True)
class InternalLink:
    node: InternalLinkNode
    to_links: list[InternalLinkNode]


@dataclasses.dataclass(frozen=True)
class ExternalLink:
    url: str
    locations: list[Location]


@dataclasses.dataclass(frozen=True)
class BackupFilePath:
    timestamp: int
    backup: pathlib.Path
    info: pathlib.Path

    def load_backup(self) -> Optional[BackupJSON]:
        return load_json(self.backup, schema=jsonschema_backup())

    def load_info(self) -> Optional[BackupInfoJSON]:
        return load_json(self.info, schema=jsonschema_backup_info())

    def load(self) -> Optional[BackupData]:
        backup = self.load_backup()
        if backup is None:
            return None
        return BackupData(
            backup=backup,
            info=self.load_info(),
        )


@dataclasses.dataclass(frozen=True)
class BackupData:
    backup: BackupJSON
    info: Optional[BackupInfoJSON]

    def __post_init__(self) -> None:
        # JSONSchema validation
        jsonschema.validate(
            instance=self.backup,
            schema=jsonschema_backup(),
        )
        if self.info is not None:
            jsonschema.validate(
                instance=self.info,
                schema=jsonschema_backup_info(),
            )


@dataclasses.dataclass(frozen=True)
class UpdateDiff:
    added: list[pathlib.Path]
    updated: list[pathlib.Path]
    removed: list[pathlib.Path]


class BackupRepository:
    def __init__(
        # pylint: disable=too-many-arguments
        self,
        project: str,
        directory: pathlib.Path,
        backup: BackupJSON,
        info: Optional[BackupInfoJSON],
        *,
        page_order: Optional[PageOrder] = None,
    ) -> None:
        self._project = project
        self._directory = directory
        self._backup = backup
        self._info = info
        self._page_order = page_order
        # sort pages
        _sort_pages(self._backup["pages"], self._page_order)
        # JSON Schema validation
        jsonschema.validate(
            instance=self._backup,
            schema=jsonschema_backup(),
        )
        if self._info is not None:
            jsonschema.validate(
                instance=self._info,
                schema=jsonschema_backup_info(),
            )

    @property
    def project(self) -> str:
        return self._project

    @property
    def directory(self) -> pathlib.Path:
        return self._directory

    @property
    def timestamp(self) -> int:
        return self._backup["exported"]

    @property
    def data(self) -> BackupJSON:
        return self._backup

    @property
    def info(self) -> Optional[BackupInfoJSON]:
        return self._info

    def page_titles(self) -> list[str]:
        return sorted(page["title"] for page in self._backup["pages"])

    def internal_links(self) -> list[InternalLink]:
        # page
        pages = {_normalize_page_title(page): page for page in self.page_titles()}
        # links
        links: list[InternalLink] = []
        for page in self._backup["pages"]:
            to_links = sorted(
                (
                    InternalLinkNode(
                        name=pages.get(link, link),
                        type=(
                            "page" if _normalize_page_title(link) in pages else "word"
                        ),
                    )
                    for link in page["linksLc"]
                ),
                key=lambda node: node.name,
            )
            links.append(
                InternalLink(
                    node=InternalLinkNode(name=page["title"], type="page"),
                    to_links=to_links,
                )
            )
        links.sort(key=lambda link: link.node.name)
        return links

    def external_links(self) -> list[ExternalLink]:
        # regex
        regex = re.compile(r"https?://[^\s\]]+")
        # links
        links: list[ExternalLink] = []
        for page in self._backup["pages"]:
            for line, location in _filter_code(page):
                for url in regex.findall(line):
                    found = next((link for link in links if link.url == url), None)
                    if found is not None:
                        found.locations.append(location)
                    else:
                        links.append(ExternalLink(url=url, locations=[location]))
        # sort
        links.sort(key=lambda link: link.url)
        for link in links:
            link.locations.sort()
        return links

    def update(
        self,
        backup: BackupJSON,
        info: Optional[BackupInfoJSON],
        *,
        logger: Optional[logging.Logger] = None,
    ) -> UpdateDiff:
        logger = logger or logging.getLogger(__name__)
        added: list[pathlib.Path] = []
        updated: list[pathlib.Path] = []
        removed: list[pathlib.Path] = []
        # sort pages
        _sort_pages(backup["pages"], self._page_order)
        # backup
        backup_path = self.directory.joinpath(f"{_escape_filename(self.project)}.json")
        if backup != self._backup:
            logger.debug(f'update "{backup_path}"')
            save_json(backup_path, backup)
            updated.append(backup_path)
        # info
        info_path = backup_path.with_suffix(".info.json")
        if info != self._info:
            if info is None:
                logger.debug(f'remove "{backup_path}"')
                info_path.unlink()
                removed.append(info_path)
            elif self._info is None:
                logger.debug(f'add "{backup_path}"')
                save_json(info_path, info)
                added.append(info_path)
            else:
                logger.debug(f'update "{backup_path}"')
                save_json(info_path, info)
                updated.append(info_path)
        # previous pages
        previous_pages = {
            _escape_filename(page["title"]): page for page in self._backup["pages"]
        }
        # add/update pages
        page_directory = self.directory.joinpath("pages")
        for page in backup["pages"]:
            title = _escape_filename(page["title"])
            page_path = page_directory.joinpath(f"{title}.json")
            if title in previous_pages:
                if page != previous_pages[title]:
                    # update page
                    logger.debug(f'update "{page_path}"')
                    save_json(page_path, page)
                    updated.append(page_path)
                # remove from dict to detect deleted pages
                del previous_pages[title]
            else:
                # add new page
                logger.debug(f'add "{page_path}"')
                save_json(page_path, page)
                added.append(page_path)
        # remove deleted pages
        for title in previous_pages.keys():
            page_path = page_directory.joinpath(f"{title}.json")
            logger.debug(f'remove "{page_path}"')
            page_path.unlink()
            removed.append(page_path)
        # update self
        self._backup = backup
        self._info = info
        return UpdateDiff(added, updated, removed)

    def save_files(self) -> list[pathlib.Path]:
        files: list[pathlib.Path] = []
        # {project}.json
        backup_path = self.directory.joinpath(f"{_escape_filename(self.project)}.json")
        files.append(backup_path)
        # {project}.info.json
        if self._info is not None:
            files.append(backup_path.with_suffix(".info.json"))
        # pages
        page_directory = self.directory.joinpath("pages")
        for page in self._backup["pages"]:
            files.append(
                page_directory.joinpath(f'{_escape_filename(page["title"])}.json')
            )
        return files

    def save(
        self,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        logger = logger or logging.getLogger(__name__)
        # {project}.json
        backup_path = self.directory.joinpath(f"{_escape_filename(self.project)}.json")
        logger.debug(f'save "{backup_path}"')
        save_json(backup_path, self._backup)
        # {project}.info.json
        if self._info is not None:
            info_path = backup_path.with_suffix(".info.json")
            logger.debug(f'save "{info_path}"')
            save_json(info_path, self._info)
        # pages
        page_directory = self.directory.joinpath("pages")
        for page in self._backup["pages"]:
            page_path = page_directory.joinpath(
                f'{_escape_filename(page["title"])}.json'
            )
            logger.debug(f'save "{page_path}"')
            save_json(page_path, page)

    @classmethod
    def load(
        cls,
        project: str,
        directory: pathlib.Path,
        *,
        page_order: Optional[PageOrder] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Optional[Self]:
        logger = logger or logging.getLogger(__name__)
        # {project}.json & {project}.info.json
        data = BackupFilePath(
            timestamp=0,  # dummy timestamp
            backup=directory.joinpath(f"{_escape_filename(project)}.json"),
            info=directory.joinpath(f"{_escape_filename(project)}.info.json"),
        ).load()
        if data is None:
            return None
        # check if pages are sorted
        if _is_sorted_pages(data.backup["pages"], page_order) is False:
            logger.warn(f"loaded backup pages are not sorted by {page_order}")
        return cls(
            project,
            directory,
            data.backup,
            data.info,
            page_order=page_order,
        )


class BackupArchive:
    def __init__(
        self,
        path: pathlib.Path,
        *,
        subdirectory: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        logger = logger or logging.getLogger(__name__)
        self._path = path
        self._directory = (
            _ArchiveDirectoryTree(path, logger)
            if subdirectory
            else _ArchiveDirectory(path)
        )

    @property
    def path(self) -> pathlib.Path:
        return self._path

    def file_path(self, timestamp: int) -> BackupFilePath:
        return self._directory.file_path(timestamp)

    def backups(self) -> list[BackupFilePath]:
        backups = self._directory.find_all()
        # sort by old...new
        return sorted(backups, key=lambda backup: backup.timestamp)


def _sort_pages(
    pages: list[BackupPageJSON],
    order: Optional[PageOrder],
) -> None:
    match order:
        case None | "as-is":
            pass
        case "created-asc":
            pages.sort(key=lambda page: page["created"])
        case "created-desc":
            pages.sort(key=lambda page: -page["created"])


def _is_sorted_pages(
    pages: list[BackupPageJSON], order: Optional[PageOrder]
) -> Optional[bool]:
    if order is None or order == "as-is":
        return None
    # sort shallow copied pages
    sorted_pages = pages[:]
    _sort_pages(sorted_pages, order)
    # compare the id of each page
    return all(id(x) == id(y) for x, y in itertools.zip_longest(pages, sorted_pages))


def _escape_filename(text: str) -> str:
    table: dict[str, int | str | None] = {
        " ": "_",
        "#": "%23",
        "%": "%25",
        "/": "%2F",
    }
    return text.translate(str.maketrans(table))


def _normalize_page_title(title: str) -> str:
    return title.lower().replace(" ", "_")


def _filter_code(page: BackupPageJSON) -> Generator[Tuple[str, Location], None, None]:
    title = page["title"]
    # regex
    code_block = re.compile(r"(?P<indent>(\t| )*)code:.+")
    cli_notation = re.compile(r"(\t| )*(\$|%) .+")
    code_snippets = re.compile(r"`.*?`")
    indent = re.compile(r"(\t| )*")
    # code block
    code_block_indent_level: Optional[int] = None
    # iterate lines
    for i, line in enumerate(page_lines(page)):
        # in code block
        if code_block_indent_level is not None:
            indent_match = indent.match(line)
            indent_level = len(indent_match.group()) if indent_match is not None else 0
            # end code block
            if indent_level <= code_block_indent_level:
                code_block_indent_level = None
            else:
                continue
        # start code_block
        if code_block_match := code_block.match(line):
            code_block_indent_level = len(code_block_match.group("indent"))
            continue
        # CLI notation
        if cli_notation.match(line):
            continue
        # code snippets
        line = code_snippets.sub(" ", line)
        yield line, Location(title=title, line=i)


class _ArchiveDirectory:
    def __init__(self, path: pathlib.Path) -> None:
        self._path = path

    def file_path(self, timestamp: int) -> BackupFilePath:
        # {timestamp}.json, {timestamp}.info.json
        return BackupFilePath(
            timestamp=timestamp,
            backup=self._path.joinpath(f"{timestamp}.json"),
            info=self._path.joinpath(f"{timestamp}.info.json"),
        )

    def find_all(self) -> list[BackupFilePath]:
        result: list[BackupFilePath] = []
        # check if the path is directory
        if not self._path.is_dir():
            return result
        # iterate files
        pattern = re.compile(r"^(?P<timestamp>[0-9]+)\.json$")
        for path in self._path.iterdir():
            if not path.is_file():
                continue
            # check if the filename is '{timestamp}.json'
            match = pattern.match(path.name)
            if match:
                timestamp = int(match.group("timestamp"))
                result.append(
                    BackupFilePath(
                        timestamp=timestamp,
                        backup=path,
                        info=path.with_suffix(".info.json"),
                    )
                )
        return result


class _ArchiveDirectoryTree:
    def __init__(
        self,
        path: pathlib.Path,
        logger: logging.Logger,
    ) -> None:
        self._path = path
        self._logger = logger

    def file_path(self, timestamp: int) -> BackupFilePath:
        sub_directory = self._path.joinpath(str(math.floor(timestamp / 1.0e7)))
        return _ArchiveDirectory(sub_directory).file_path(timestamp)

    def find_all(self) -> list[BackupFilePath]:
        result: list[BackupFilePath] = []
        # check if the path is directory
        if not self._path.is_dir():
            return result
        # iterate directories
        pattern = re.compile(r"^(?P<quotient>[0-9]+)$")
        for path in self._path.iterdir():
            if not path.is_dir():
                continue
            # check if the directory name is 'timestamp / 1.0e+7'
            match = pattern.match(path.name)
            if match:
                quotient = int(match.group("quotient"))
                file_paths = _ArchiveDirectory(path).find_all()
                if any(
                    math.floor(file_path.timestamp / 1.0e7) != quotient
                    for file_path in file_paths
                ):
                    self._logger.warning(f'unexpected files exist in "{path}"')
                result.extend(file_paths)
        return result
