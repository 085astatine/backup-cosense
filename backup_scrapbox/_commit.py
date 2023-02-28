import datetime
import logging
import pathlib
from typing import Optional
from ._backup import Backup, BackupJSONs, BackupStorage
from ._config import Config, GitEmptyInitialCommitConfig
from ._external_link import save_external_links
from ._git import Commit, CommitTarget, Git
from ._utility import format_timestamp
from .exceptions import InitialCommitError


def commit_backups(
        config: Config,
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    git = config.git.git(logger=logger)
    storage = BackupStorage(pathlib.Path(config.scrapbox.save_directory))
    # backup targets
    backup_targets = _backup_targets(storage, git, logger)
    # commit
    for target in backup_targets:
        logger.info(f'commit {format_timestamp(target.timestamp)}')
        # load backup
        backup = target.load(
                config.scrapbox.project,
                git.path,
                page_order=config.git.page_order)
        if backup is None:
            logger.info(
                    'failed to load backup'
                    f' {format_timestamp(target.timestamp)}')
            continue
        # commit
        commit_backup(config, backup, logger=logger)


def commit_backup(
        config: Config,
        backup: Backup,
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    git = config.git.git(logger=logger)
    # git init
    if not git.exists():
        logger.info('create git repository "{git.path}"')
        git.init()
    # initial commit
    _initial_commit(config, git, [backup])
    # load previous backup
    previous_backup = Backup.load(
            backup.project,
            git.path,
            page_order=config.git.page_order,
            logger=logger)
    # update backup json
    target = _update_backup_json(backup, previous_backup, logger)
    # external link
    if config.external_link.enabled:
        target.update(save_external_links(
                backup,
                git.path,
                config=config.external_link,
                logger=logger))
    # commit message
    message = Commit.message(
            backup.project,
            backup.timestamp,
            backup.info)
    # commit
    git.commit(
            target,
            message,
            timestamp=backup.timestamp)


def staging_backup(
        config: Config,
        data: BackupJSONs,
        *,
        backup: Optional[Backup] = None,
        logger: Optional[logging.Logger] = None) -> Optional[CommitTarget]:
    logger = logger or logging.getLogger(__name__)
    # git switch
    git = config.git.git(logger=logger)
    if git.exists():
        git.switch(allow_orphan=True)
    # load backup repository
    if backup is None:
        backup = Backup.load(
                config.scrapbox.project,
                git.path,
                page_order=config.git.page_order,
                logger=logger)
    # load json
    backup_json = data.load_backup()
    info_json = data.load_info()
    if backup_json is None:
        logger.error('failure to load "{data.backup_path}"')
        return None
    # update backup
    if backup is None:
        # initial update
        backup = Backup(
                config.scrapbox.project,
                git.path,
                backup_json,
                info_json,
                page_order=config.git.page_order)
        backup.save(logger=logger)
        commit_target = CommitTarget(updated=set(backup.save_files()))
    else:
        # update
        update_diff = backup.update(
                backup_json,
                info_json,
                logger=logger)
        commit_target = CommitTarget(
                added=set(update_diff.added),
                updated=set(update_diff.updated),
                deleted=set(update_diff.removed))
    # external links
    if config.external_link.enabled:
        commit_target.update(save_external_links(
                backup,
                git.path,
                config=config.external_link,
                logger=logger))
    return commit_target


def _backup_targets(
        storage: BackupStorage,
        git: Git,
        logger: logging.Logger) -> list[BackupJSONs]:
    # get latest backup timestamp
    latest = git.latest_commit_timestamp()
    logger.info(f'latest backup: {format_timestamp(latest)}')
    # find backup
    targets = [
            backup for backup in storage.backups()
            if latest is None or latest < backup.timestamp]
    return targets


def _update_backup_json(
        backup: Backup,
        previous_backup: Optional[Backup],
        logger: logging.Logger) -> CommitTarget:
    # previous files
    previous_files = set(
            previous_backup.save_files()
            if previous_backup is not None
            else [])
    # clear previous files
    for previous_file in previous_files:
        logger.debug(f'remove "{previous_file.as_posix()}"')
        previous_file.unlink()
    # next files
    next_files = set(backup.save_files())
    # copy next files
    backup.save(logger=logger)
    # commit target
    return CommitTarget(
            added=next_files - previous_files,
            updated=next_files & previous_files,
            deleted=previous_files - next_files)


def _initial_commit(
        config: Config,
        git: Git,
        backups: list[Backup]) -> None:
    # empty initial commit is enabled
    if config.git.empty_initial_commit is None:
        return
    # branch already exists
    if config.git.branch in git.branches():
        return
    # commit
    timestamp = _initial_commit_timestamp(
            config.git.empty_initial_commit,
            backups)
    git.commit(
            CommitTarget(),
            config.git.empty_initial_commit.message,
            timestamp=timestamp)


def _initial_commit_timestamp(
        config: GitEmptyInitialCommitConfig,
        backups: list[Backup]) -> int:
    match config.timestamp:
        case datetime.datetime():
            return int(config.timestamp.timestamp())
        case datetime.date():
            # add time(00:00:00) to date
            return int(datetime.datetime.combine(
                    config.timestamp,
                    datetime.time()).timestamp())
        case 'oldest_backup':
            timestamp = min(
                    (backup.timestamp for backup in backups),
                    default=None)
            if timestamp is None:
                raise InitialCommitError(
                    'Since there is no backup, '
                    'unable to define timestamp')
            return timestamp
        case 'oldest_created_page':
            backup = min(
                    backups,
                    default=None,
                    key=lambda backup: backup.timestamp)
            if backup is None:
                raise InitialCommitError(
                    'Since there is no backup, '
                    'unable to define timestamp')
            timestamp = min(
                    (page['created'] for page in backup.data['pages']),
                    default=None)
            if timestamp is None:
                raise InitialCommitError(
                    'There is no page in oldest backup'
                    f' ({format_timestamp(backup.timestamp)})')
            return timestamp
