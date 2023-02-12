import logging
import pathlib
import subprocess
from typing import Any, Optional
from ._backup import jsonschema_backup
from ._config import Config
from ._git import Git, Commit
from ._json import parse_json, save_json
from ._utility import format_timestamp


def export_backups(
        config: Config,
        destination: pathlib.Path,
        logger: logging.Logger) -> None:
    git = config.git.git(logger=logger)
    # check if the destination exists
    if not destination.exists():
        logger.error(
                f'export directory "{destination.as_posix()}" does not exist')
        return
    # commits
    commits = git.commits()
    if commits:
        logger.info(
                f'{len(commits)} commits:'
                f' {format_timestamp(commits[0].timestamp)}'
                f' ~ {format_timestamp(commits[-1].timestamp)}')
    else:
        logger.info('there are no commits')
    # export
    for commit in commits:
        logger.info(f'export {format_timestamp(commit.timestamp)}')
        _export(config.scrapbox.project,
                git,
                commit,
                destination,
                logger)


def _export(
        project: str,
        git: Git,
        commit: Commit,
        destination: pathlib.Path,
        logger: logging.Logger) -> None:
    # save backup.json
    backup_path = destination.joinpath(f'{commit.timestamp}.json')
    if not _export_json(
            git,
            commit.hash,
            f'{project}.json',
            backup_path,
            jsonschema_backup()):
        logger.warning(f'skip commit: {commit.hash}')
        return
    logger.debug(f'save "{backup_path}"')
    # save backup.info.json
    info_json_path = destination.joinpath(f'{commit.timestamp}.info.json')
    info_json = commit.backup_info()
    if info_json is not None:
        logger.debug(f'save "{info_json_path}"')
        save_json(info_json_path, info_json)


def _export_json(
        git: Git,
        commit_hash: str,
        file: str,
        output: pathlib.Path,
        schema: Optional[dict[str, Any]]) -> bool:
    # get from git
    command = ['git', 'show', '-z', f'{commit_hash}:{file}']
    try:
        process = git.execute(command)
    except subprocess.CalledProcessError:
        return False
    # save
    save_json(
            output,
            parse_json(process.stdout),
            schema=schema)
    return True
