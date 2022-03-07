import datetime
import logging
import pathlib
from typing import Optional, Union
from ._backup import Backup, BackupStorage
from ._env import Env, PageOrder
from ._git import Commit, CommitTarget, Git
from ._json import BackupJSON, jsonschema_backup, load_json, save_json
from ._utility import format_timestamp


def commit(
        env: Env,
        logger: logging.Logger) -> None:
    git = env.git(logger=logger)
    storage = env.backup_storage()
    # check if the git repository exists
    if not git.exists():
        logger.error('git repository "%s" does not exist', git.path)
        return
    # backup targets
    backup_targets = _backup_targets(
            storage,
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
                info,
                env.page_order,
                logger)
        # commit
        _commit(env.project,
                git,
                info,
                commit_targets)


def _backup_targets(
        storage: BackupStorage,
        git: Git,
        logger: logging.Logger) -> list[Backup]:
    # get latest backup timestamp
    latest = git.latest_commit_timestamp()
    logger.info('latest backup: %s', format_timestamp(latest))
    # find backup
    targets = [
            backup for backup in storage.backups()
            if latest is None or latest < backup.timestamp]
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
                git.execute(['git', 'rm', target.as_posix()])
            else:
                logger.debug('rm %s', target)
                target.unlink()
    # rm pages/
    if page_directory.exists() and not list(page_directory.iterdir()):
        page_directory.rmdir()


def _copy_backup(
        project: str,
        git: Git,
        backup_info: Backup,
        page_order: Optional[PageOrder],
        logger: logging.Logger) -> list[pathlib.Path]:
    copied: list[pathlib.Path] = []
    # load backup
    backup = backup_info.load_backup()
    if backup is None:
        logger.error('failed to load "%s"', backup_info.backup_path)
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
        backup: Backup,
        targets: list[pathlib.Path]) -> None:
    # commit message
    message = Commit.message(project, backup.timestamp, backup.load_info())
    # commit
    git.commit(
            CommitTarget(added=targets),
            message,
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
