import logging
import pathlib
from typing import Optional
from ._backup import Backup, BackupStorage, DownloadedBackup
from ._config import Config
from ._external_link import save_external_links
from ._git import Commit, CommitTarget, Git
from ._utility import format_timestamp


def commit_backups(
        config: Config,
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    git = Git(pathlib.Path(config.git.path), logger=logger)
    storage = BackupStorage(pathlib.Path(config.scrapbox.save_directory))
    # check if the git repository exists
    if not git.exists():
        logger.error(f'git repository "{git.path}" does not exist')
        return
    # backup targets
    backup_targets = _backup_targets(storage, git, logger)
    # commit
    for target in backup_targets:
        logger.info(f'commit {format_timestamp(target.timestamp)}')
        # load backup
        backup = target.load(config.scrapbox.project, git.path)
        if backup is None:
            logger.info(
                    'failed to load backup'
                    f' {format_timestamp(target.timestamp)}')
            continue
        # sort pages
        backup.sort_pages(config.git.page_order)
        # commit
        commit_backup(config, backup, logger=logger)


def commit_backup(
        config: Config,
        backup: Backup,
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    git = Git(pathlib.Path(config.git.path), logger=logger)
    # load previous backup
    previous_backup = Backup.load(backup.project, git.path)
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


def _backup_targets(
        storage: BackupStorage,
        git: Git,
        logger: logging.Logger) -> list[DownloadedBackup]:
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
