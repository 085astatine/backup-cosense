from __future__ import annotations

import dataclasses
import datetime
import logging
import pathlib
import subprocess
import sys
from typing import Any, Optional

from ._backup import BackupArchive, jsonschema_backup, jsonschema_backup_info
from ._config import Config
from ._git import Commit, Git
from ._json import parse_json, save_json
from ._utility import format_timestamp


def export_backups(
    config: Config,
    destination: BackupArchive,
    *,
    dry_run: bool = False,
    after: Optional[datetime.datetime] = None,
    before: Optional[datetime.datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    logger = logger or logging.getLogger(__name__)
    git = config.git.create(logger=logger)
    # check if the destination exists
    if not destination.path.exists():
        logger.error(f'export directory "{destination.path}" does not exist')
        return
    # commits
    export_range = _ExportRange(
        after=after,
        before=before,
    )
    commits = git.commits(option=export_range.to_option())
    if commits:
        logger.info(
            f"{len(commits)} commits:"
            f" {format_timestamp(commits[0].timestamp)}"
            f" ~ {format_timestamp(commits[-1].timestamp)}"
        )
    else:
        logger.info("there are no commits")
    # export
    for commit in commits:
        logger.info(f"export {format_timestamp(commit.timestamp)}")
        if dry_run:
            _dry_run_export(
                config.cosense.project,
                git,
                commit,
                destination,
            )
            continue
        _export(
            config.cosense.project,
            git,
            commit,
            destination,
            logger,
        )


@dataclasses.dataclass(frozen=True)
class _ExportRange:
    after: Optional[datetime.datetime]
    before: Optional[datetime.datetime]

    def to_option(self) -> list[str]:
        option: list[str] = []
        if self.after is not None:
            option.extend(["--after", self.after.isoformat()])
        if self.before is not None:
            option.extend(["--before", self.before.isoformat()])
        return option


def _export(
    project: str,
    git: Git,
    commit: Commit,
    destination: BackupArchive,
    logger: logging.Logger,
) -> None:
    target = _export_target(project, git, commit)
    file_path = destination.file_path(commit.timestamp)
    # save {project}.json
    backup_object = target.backup_object()
    if backup_object is None:
        logger.warning(f"skip commit: {commit.hash}")
        return
    if not _export_json(
        git,
        backup_object,
        file_path.backup,
        jsonschema_backup(),
    ):
        logger.warning(f"skip commit: {commit.hash}")
        return
    logger.debug(f'save "{file_path.backup}"')
    # save {project}.info.json
    info_object = target.info_object()
    if info_object is None:
        # from commit message
        logger.debug("generate info.json from commit message")
        info_json = commit.backup_info()
        if info_json is not None:
            save_json(file_path.info, info_json)
            logger.debug(f'save "{file_path.info}"')
    elif _export_json(
        git,
        info_object,
        file_path.info,
        jsonschema_backup_info(),
    ):
        logger.debug(f'save "{file_path.info}"')


def _dry_run_export(
    project: str,
    git: Git,
    commit: Commit,
    destination: BackupArchive,
) -> None:
    target = _export_target(project, git, commit)
    file_path = destination.file_path(commit.timestamp)
    # {project}.json
    if target.has_backup is None:
        return
    sys.stdout.write(f"(dry-run) export: {file_path.backup}\n")
    # {project}.info.json
    if target.has_info is None:
        info = commit.backup_info()
        if info is None:
            return
    sys.stdout.write(f"(dry-run) export: {file_path.info}\n")


@dataclasses.dataclass(frozen=True)
class _ExportTarget:
    commit: str
    project: str
    has_backup: bool
    has_info: bool

    def backup_object(self) -> Optional[str]:
        if self.has_backup:
            return f"{self.commit}:{self.project}.json"
        return None

    def info_object(self) -> Optional[str]:
        if self.has_info:
            return f"{self.commit}:{self.project}.info.json"
        return None


def _export_target(
    project: str,
    git: Git,
    commit: Commit,
) -> _ExportTarget:
    # add / modified files in commit
    command = [
        "git",
        "show",
        "-z",
        "--name-only",
        "--diff-filter=MA",
        '--format=tformat:""',
        f"{commit.hash}",
    ]
    try:
        process = git.execute(command)
    except subprocess.CalledProcessError:
        return _ExportTarget(
            commit=commit.hash,
            project=project,
            has_backup=False,
            has_info=False,
        )
    names = [x for x in process.stdout.removeprefix('""\0\n').split("\0") if x != ""]
    # {project}.json & {project}.info.json
    return _ExportTarget(
        project=project,
        commit=commit.hash,
        has_backup=f"{project}.json" in names,
        has_info=f"{project}.info.json" in names,
    )


def _export_json(
    git: Git,
    target: str,
    output: pathlib.Path,
    schema: Optional[dict[str, Any]],
) -> bool:
    # get from git
    command = ["git", "show", target]
    try:
        process = git.execute(command)
    except subprocess.CalledProcessError:
        return False
    # save
    save_json(output, parse_json(process.stdout), schema=schema)
    return True
