import logging
import pathlib
import subprocess
from typing import Optional
from ._backup import BackupJSON, jsonschema_backup
from ._config import Config
from ._git import Git, Commit
from ._json import parse_json, save_json
from ._utility import format_timestamp


def export_backups(
        config: Config,
        destination: pathlib.Path,
        logger: logging.Logger) -> None:
    git = Git(pathlib.Path(config.git.path), logger=logger)
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
    # get backup.json
    command = ['git', 'show', '-z', f'{commit.hash}:{project}.json']
    try:
        process = git.execute(command)
    except subprocess.CalledProcessError:
        logger.warning(f'skip commit: {commit.hash}')
        return
    backup_json: Optional[BackupJSON] = parse_json(
            process.stdout,
            schema=jsonschema_backup())
    # save backup.json
    backup_json_path = destination.joinpath(f'{commit.timestamp}.json')
    save_json(backup_json_path, backup_json)
    logger.debug(f'save "{backup_json_path}"')
    # save backup.info.json
    info_json_path = destination.joinpath(f'{commit.timestamp}.info.json')
    info_json = commit.backup_info()
    if info_json is not None:
        logger.debug(f'save "{info_json_path}"')
        save_json(info_json_path, info_json)
