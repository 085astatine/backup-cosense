from __future__ import annotations

import dataclasses
import itertools
import logging
import math
import pathlib
import re
from typing import Any, Generator, Literal, Optional, Tuple, TypedDict

import jsonschema

from ._json import load_json, save_json
from ._utility import CommitTarget

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

    @property
    def timestamp(self) -> int:
        return self.backup["exported"]

    @property
    def pages(self) -> list[BackupPageJSON]:
        return self.backup["pages"]

    def page_titles(self) -> list[str]:
        return sorted(page["title"] for page in self.backup["pages"])

    def internal_links(self) -> list[InternalLink]:
        # page
        pages = {_normalize_page_title(page): page for page in self.page_titles()}
        # links
        links: list[InternalLink] = []
        for page in self.backup["pages"]:
            to_links = [
                InternalLinkNode(
                    name=pages.get(link, link),
                    type="page" if _normalize_page_title(link) in pages else "word",
                )
                for link in page["linksLc"]
            ]
            to_links.sort(key=lambda node: node.name)
            links.append(
                InternalLink(
                    node=InternalLinkNode(name=page["title"], type="page"),
                    to_links=to_links,
                )
            )
        links.sort(key=lambda link: link.node.name)
        return links

    def external_links(self) -> list[ExternalLink]:
        # regex pattern
        pattern = re.compile(r"https?://[^\s\]]+")
        # links
        links: list[ExternalLink] = []
        for page in self.backup["pages"]:
            for line, location in _filter_code(page):
                for url in pattern.findall(line):
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

    def sort_pages(self, order: Optional[PageOrder]) -> None:
        _sort_pages(self.backup["pages"], order)

    def is_pages_sorted(self, order: Optional[PageOrder]) -> Optional[bool]:
        if order is None or order == "as-is":
            return None
        # sort shallow copied pages
        sorted_pages = self.backup["pages"][:]
        _sort_pages(sorted_pages, order)
        # compare the id of each page
        return all(
            id(x) == id(y)
            for x, y in itertools.zip_longest(self.backup["pages"], sorted_pages)
        )

    def save(
        self,
        path: BackupFilePath,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        logger = logger or logging.getLogger(__name__)
        # save backup
        logger.debug(f'save "{path.backup}"')
        save_json(path.backup, self.backup)
        # save info
        if self.info is not None:
            logger.debug(f'save "{path.info}"')
            save_json(path.info, self.info)


class BackupRepository:
    def __init__(
        # pylint: disable=too-many-arguments
        self,
        project: str,
        directory: pathlib.Path,
        *,
        data: Optional[BackupData] = None,
        page_order: Optional[PageOrder] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._project = project
        self._directory = directory
        self._data = data
        self._page_order = page_order
        self._logger = logger or logging.getLogger(__name__)
        # sort pages
        if self._data is not None:
            self._data.sort_pages(self._page_order)

    @property
    def project(self) -> str:
        return self._project

    @property
    def directory(self) -> pathlib.Path:
        return self._directory

    @property
    def data(self) -> Optional[BackupData]:
        return self._data

    def update(self, data: BackupData) -> CommitTarget:
        # sort pages
        data.sort_pages(self._page_order)
        # update backup
        file_path = _project_to_file_path(self.directory, self.project)
        updated = _update_backup(file_path, data, self._data, self._logger)
        # update pages
        page_directory = self.directory.joinpath("pages")
        updated.update(_update_pages(page_directory, data, self.data, self._logger))
        # update self
        self._data = data
        return updated

    def load(self) -> None:
        # load {project}.json & {project}.info.json
        data = _project_to_file_path(self.directory, self.project).load()
        if data is not None:
            # check if pages are sorted
            if data.is_pages_sorted(self._page_order) is False:
                self._logger.warn(
                    f"loaded backup pages are not sorted by {self._page_order}"
                )
            self._data = data

    def save(self) -> None:
        if self._data is None:
            return
        # {project}.json & {project}.info.json
        file_path = _project_to_file_path(self.directory, self.project)
        self._data.save(file_path, logger=self._logger)
        # pages
        page_directory = self.directory.joinpath("pages")
        for page in self._data.backup["pages"]:
            page_path = _page_to_file_path(page_directory, page)
            self._logger.debug(f'save "{page_path}"')
            save_json(page_path, page)


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


def _project_to_file_path(
    directory: pathlib.Path,
    project: str,
) -> BackupFilePath:
    filename = _escape_filename(project)
    return BackupFilePath(
        timestamp=0,  # dummy timestamp
        backup=directory.joinpath(f"{filename}.json"),
        info=directory.joinpath(f"{filename}.info.json"),
    )


def _page_to_file_path(
    directory: pathlib.Path,
    page: BackupPageJSON,
) -> pathlib.Path:
    filename = _escape_filename(page["title"])
    return directory.joinpath(f"{filename}.json")


def _update_backup(
    file_path: BackupFilePath,
    current: BackupData,
    previous: Optional[BackupData],
    logger: logging.Logger,
) -> CommitTarget:
    added: set[pathlib.Path] = set()
    updated: set[pathlib.Path] = set()
    deleted: set[pathlib.Path] = set()
    if previous is None:
        added.add(file_path.backup)
        if current.info is not None:
            added.add(file_path.info)
    else:
        # backup
        if current.backup != previous.backup:
            logger.debug(f'update "{file_path.backup}"')
            updated.add(file_path.backup)
        # info
        if current.info != previous.info:
            if current.info is None:
                logger.debug(f'delete "{file_path.info}"')
                if file_path.info.exists():
                    file_path.info.unlink()
                deleted.add(file_path.info)
            elif previous.info is None:
                logger.debug(f'add "{file_path.info}"')
                added.add(file_path.info)
            else:
                logger.debug(f'update "{file_path.info}"')
                updated.add(file_path.info)
    # save
    current.save(file_path, logger=logger)
    return CommitTarget(added=added, updated=updated, deleted=deleted)


def _update_pages(
    directory: pathlib.Path,
    current: BackupData,
    previous: Optional[BackupData],
    logger: logging.Logger,
) -> CommitTarget:
    added: set[pathlib.Path] = set()
    updated: set[pathlib.Path] = set()
    deleted: set[pathlib.Path] = set()
    # diff
    pages_diff = _diff_pages(current, previous)
    # deleted pages
    for page in pages_diff.deleted:
        page_path = _page_to_file_path(directory, page)
        logger.debug(f'delete "{page_path}"')
        page_path.unlink()
        deleted.add(page_path)
    # updated pages
    for page in pages_diff.updated:
        page_path = _page_to_file_path(directory, page)
        logger.debug(f'update "{page_path}"')
        save_json(page_path, page, schema=jsonschema_backup_page())
        updated.add(page_path)
    # added pages
    for page in pages_diff.added:
        page_path = _page_to_file_path(directory, page)
        logger.debug(f'add "{page_path}"')
        save_json(page_path, page, schema=jsonschema_backup_page())
        added.add(page_path)
    return CommitTarget(added=added, updated=updated, deleted=deleted)


@dataclasses.dataclass(frozen=True)
class PagesDiff:
    added: list[BackupPageJSON]
    updated: list[BackupPageJSON]
    deleted: list[BackupPageJSON]


def _diff_pages(
    current: BackupData,
    previous: Optional[BackupData],
) -> PagesDiff:
    added: list[BackupPageJSON] = []
    updated: list[BackupPageJSON] = []
    deleted: list[BackupPageJSON] = []
    if previous is None:
        added.extend(current.pages)
    else:
        pages = {_escape_filename(page["title"]): page for page in previous.pages}
        for page in current.pages:
            title = _escape_filename(page["title"])
            # added
            if title not in pages:
                added.append(page)
            else:
                # upated
                if page != pages[title]:
                    updated.append(page)
                del pages[title]
        # delted
        deleted.extend(pages.values())
    return PagesDiff(added=added, updated=updated, deleted=deleted)


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
