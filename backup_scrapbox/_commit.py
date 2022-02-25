import dataclasses
import datetime
import logging
import pathlib
import re
from typing import Optional, Union
from ._env import Env, PageOrder
from ._git import Git
from ._json import (
        BackupJSON, BackupInfoJSON, jsonschema_backup, jsonschema_backup_info,
        load_json, save_json)
from ._utility import format_timestamp


def commit(
        env: Env,
        logger: logging.Logger) -> None:
    git = env.git(logger=logger)
    backup_directory = pathlib.Path(env.save_directory)
    # check if the git repository exists
    if not git.exists():
        logger.error('git repository "%s" does not exist', git.path)
        return
    # switch Git branch
    if env.git_branch is not None:
        logger.info('switch git branch "%s"', env.git_branch)
        git.command(['git', 'switch', env.git_branch])
    # backup targets
    backup_targets = _backup_targets(
            backup_directory,
            git,
            logger)
    # commit
    for info in backup_targets:
        logger.info('commit %s', format_timestamp(info.timestamp))
        # clear
        _clear_repository(
                env.project,
                git,
                logger)
        # copy
        commit_targets = _copy_backup(
                env.project,
                git,
                info.backup_path,
                env.page_order,
                logger)
        # commit
        _commit(env.project,
                git,
                info,
                commit_targets)


@dataclasses.dataclass
class _Backup:
    timestamp: int
    backup_path: pathlib.Path
    info_path: Optional[pathlib.Path]


def _backup_targets(
        directory: pathlib.Path,
        git: Git,
        logger: logging.Logger) -> list[_Backup]:
    # get latest backup timestamp
    latest_timestamp = git.latest_commit_timestamp()
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
    targets.sort(key=lambda x: x.timestamp)
    return targets


def _clear_repository(
        project: str,
        git: Git,
        logger: logging.Logger) -> None:
    targets: list[pathlib.Path] = []
    # load previous backup
    previous_backup_path = git.path.joinpath(
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
    page_directory = git.path.joinpath('pages')
    targets.extend(
            page_directory.joinpath(f'{_escape_filename(page["title"])}.json')
            for page in previous_backup['pages'])
    # git ls-files
    staged = git.ls_files()
    # rm / git rm
    for target in targets:
        if target.exists():
            if target in staged:
                git.command(['git', 'rm', target.as_posix()])
            else:
                logger.debug('rm %s', target)
                target.unlink()
    # rm pages/
    if page_directory.exists() and not list(page_directory.iterdir()):
        page_directory.rmdir()


def _copy_backup(
        project: str,
        git: Git,
        backup_path: pathlib.Path,
        page_order: Optional[PageOrder],
        logger: logging.Logger) -> list[pathlib.Path]:
    copied: list[pathlib.Path] = []
    # load backup
    backup: Optional[BackupJSON] = load_json(backup_path)
    if backup is None:
        logger.error('failed to load "%s"', backup_path)
        return copied
    _sort_pages(backup, page_order)
    # copy
    logger.info(
            "copy backup created at %s",
            format_timestamp(backup['exported']))
    # copy: ${project}.json
    backup_path = git.path.joinpath(f'{_escape_filename(project)}.json')
    logger.debug('save "%s"', backup_path)
    save_json(
            git.path.joinpath(f'{_escape_filename(project)}.json'),
            backup)
    copied.append(backup_path)
    # copy: page/${title}.json
    page_directory = git.path.joinpath('pages')
    for page in backup['pages']:
        page_path = page_directory.joinpath(
                f'{_escape_filename(page["title"])}.json')
        logger.debug('save "%s"', page_path)
        save_json(page_path, page)
        copied.append(page_path)
    return copied


def _commit(
        project: str,
        git: Git,
        backup: _Backup,
        targets: list[pathlib.Path]) -> None:
    # git add
    for target in targets:
        git.command(['git', 'add', target.as_posix()])
    # commit message
    message: list[str] = []
    message.append('{0} {1}'.format(
            project,
            datetime.datetime.fromtimestamp(backup.timestamp)))
    if backup.info_path is not None:
        info: Optional[BackupInfoJSON] = load_json(
                backup.info_path,
                schema=jsonschema_backup_info())
        if info is not None:
            message.append('')
            message.extend(
                    f'{repr(key)}: {repr(value)}'
                    for key, value in info.items())
    # commit
    git.commit(
            '\n'.join(message),
            timestamp=backup.timestamp)


def _sort_pages(
        backup: BackupJSON,
        page_order: Optional[PageOrder]) -> None:
    if page_order in (None, 'as-is'):
        pass
    elif page_order == 'created-asc':
        # sort pages by created (asc)
        backup['pages'].sort(key=lambda page: page['created'])
    elif page_order == 'created-desc':
        # sort pages by created (desc)
        backup['pages'].sort(key=lambda page: - page['created'])


def _escape_filename(text: str) -> str:
    table: dict[str, Union[int, str, None]] = {
            ' ': '_',
            '#': '%23',
            '%': '%25',
            '/': '%2F'}
    return text.translate(str.maketrans(table))
