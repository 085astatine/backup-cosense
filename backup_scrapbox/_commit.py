# -*- coding: utf-8 -*-

import datetime
import logging
import pathlib
import re
from typing import Optional, TypedDict, Union
from ._env import Env
from ._json import (
        BackupJSON, BackupInfoJSON, jsonschema_backup, jsonschema_backup_info,
        load_json, save_json)
from ._git import (
        git_command, git_commit, git_ls_files, git_show_latest_timestamp,
        is_git_repository)
from ._utility import format_timestamp


def commit(
        env: Env,
        logger: logging.Logger) -> None:
    git_repository = pathlib.Path(env['git_repository'])
    backup_directory = pathlib.Path(env['save_directory'])
    # check if the git repository exists
    if not is_git_repository(git_repository):
        logger.error('git repository "%s" does not exist', git_repository)
        return
    # switch Git branch
    if env['git_branch'] is not None:
        logger.info('switch git branch "%s"', env['git_branch'])
        git_command(
                ['git', 'switch', env['git_branch']],
                git_repository,
                logger=logger)
    # backup targets
    backup_targets = _backup_targets(
            backup_directory,
            git_repository,
            logger)
    # commit
    for info in backup_targets:
        logger.info('commit %s', format_timestamp(info['timestamp']))
        # clear
        _clear_repository(
                env['project'],
                git_repository,
                logger)
        # copy
        commit_targets = _copy_backup(
                env['project'],
                git_repository,
                info['backup_path'],
                logger)
        # commit
        _commit(env['project'],
                git_repository,
                info,
                commit_targets,
                logger)


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
    logger.info('latest backup: %s', format_timestamp(latest_timestamp))
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


def _clear_repository(
        project: str,
        git_repository: pathlib.Path,
        logger: logging.Logger) -> None:
    targets: list[pathlib.Path] = []
    # load previous backup
    previous_backup_path = git_repository.joinpath(
            f'{_escape_filename(project)}.json')
    logger.debug('load previous backup "%s"', previous_backup_path)
    previous_backup: Optional[BackupJSON] = load_json(
            previous_backup_path,
            schema=jsonschema_backup())
    if previous_backup is None:
        logger.debug('previous backup dose not exist')
        return
    targets.append(previous_backup_path)
    # pages/
    page_directory = git_repository.joinpath('pages')
    targets.extend(
            page_directory.joinpath(f'{_escape_filename(page["title"])}.json')
            for page in previous_backup['pages'])
    # git ls-files
    staged = git_ls_files(git_repository, logger=logger)
    # rm / git rm
    for target in targets:
        if target.exists():
            if target in staged:
                git_command(
                        ['git', 'rm', target.as_posix()],
                        git_repository,
                        logger=logger)
            else:
                logger.debug('rm %s', target)
                target.unlink()
    # rm pages/
    if page_directory.exists() and not list(page_directory.iterdir()):
        page_directory.rmdir()


def _copy_backup(
        project: str,
        git_repository: pathlib.Path,
        backup_path: pathlib.Path,
        logger: logging.Logger) -> list[pathlib.Path]:
    copied: list[pathlib.Path] = []
    # load backup
    backup: Optional[BackupJSON] = load_json(backup_path)
    if backup is None:
        logger.error('failed to load "%s"', backup_path)
        return copied
    # copy
    logger.info(
            "copy backup created at %s",
            format_timestamp(backup['exported']))
    # copy: ${project}.json
    backup_path = git_repository.joinpath(f'{_escape_filename(project)}.json')
    logger.debug('save "%s"', backup_path)
    save_json(
            git_repository.joinpath(f'{_escape_filename(project)}.json'),
            backup)
    copied.append(backup_path)
    # copy: page/${title}.json
    page_directory = git_repository.joinpath('pages')
    for page in backup['pages']:
        page_path = page_directory.joinpath(
                f'{_escape_filename(page["title"])}.json')
        logger.debug('save "%s"', page_path)
        save_json(page_path, page)
        copied.append(page_path)
    return copied


def _commit(
        project: str,
        git_repository: pathlib.Path,
        backup: _Backup,
        targets: list[pathlib.Path],
        logger: logging.Logger) -> None:
    # git add
    for target in targets:
        command = ['git', 'add', target.as_posix()]
        git_command(command, git_repository, logger=logger)
    # commit message
    message: list[str] = []
    message.append('{0} {1}'.format(
            project,
            datetime.datetime.fromtimestamp(backup['timestamp'])))
    if backup['info_path'] is not None:
        info: Optional[BackupInfoJSON] = load_json(
                backup['info_path'],
                schema=jsonschema_backup_info())
        if info is not None:
            message.append('')
            message.extend(
                    f'{repr(key)}: {repr(value)}'
                    for key, value in info.items())
    # commit
    git_commit(
            git_repository,
            '\n'.join(message),
            timestamp=backup['timestamp'],
            logger=logger)


def _escape_filename(text: str) -> str:
    table: dict[str, Union[int, str, None]] = {
            ' ': '_',
            '#': '%23',
            '%': '%25',
            '/': '%2F'}
    return text.translate(str.maketrans(table))
