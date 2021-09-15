# -*- coding: utf-8 -*-

import datetime
import logging
import pathlib
import re
from typing import Optional, TypedDict
from ._env import Env
from ._git import git_show_latest_timestamp, is_git_repository


def commit(
        env: Env,
        logger: logging.Logger) -> None:
    git_repository = pathlib.Path(env['git_repository'])
    backup_directory = pathlib.Path(env['save_directory'])
    # check if the git repository exists
    if not is_git_repository(git_repository):
        logger.error('git repository "%s" does not exist', git_repository)
        return None
    # backup targets
    targets = _backup_targets(
            backup_directory,
            git_repository,
            logger)
    print(targets)


class _Backup(TypedDict):
    timestamp: int
    backup_path: pathlib.Path
    info_path: Optional[pathlib.Path]


def _backup_targets(
        directory: pathlib.Path,
        git_repository: pathlib.Path,
        logger: logging.Logger) -> list[_Backup]:
    # get latest backup timestamp
    latest_timestamp = git_show_latest_timestamp(
            git_repository,
            logger=logger)
    logger.info(
            'latest backup: %s (%s)',
            datetime.datetime.fromtimestamp(latest_timestamp)
            if latest_timestamp is not None else None,
            latest_timestamp)
    # find backup
    targets: list[_Backup] = []
    for path in directory.iterdir():
        # check if the path is file
        if not path.is_file():
            continue
        # check if the filename is '${timestamp}.json'
        match = re.match(
            r'^(?P<timestamp>\d+)\.json$',
            path.name)
        if match is None:
            continue
        timestamp = int(match.group('timestamp'))
        # check if it is newer than the latest backup
        if (latest_timestamp is not None
                and timestamp <= latest_timestamp):
            continue
        # add to targets
        info_path = directory.joinpath(f'{timestamp}.info.json')
        targets.append(_Backup(
                timestamp=timestamp,
                backup_path=path,
                info_path=info_path if info_path.exists() else None))
    # sort by oldest timestamp
    targets.sort(key=lambda x: x['timestamp'])
    return targets
