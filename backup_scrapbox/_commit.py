import datetime
import logging
from typing import Optional

from ._backup import Backup, BackupJSONs
from ._config import Config, GitEmptyInitialCommitConfig
from ._external_link import save_external_links
from ._git import Commit, CommitTarget, Git
from ._utility import format_timestamp
from .exceptions import InitialCommitError


def commit_backups(
    config: Config,
    *,
    logger: Optional[logging.Logger] = None,
) -> None:
    logger = logger or logging.getLogger(__name__)
    git = config.git.git(logger=logger)
    backup: Optional[Backup] = None
    # git switch
    if git.exists():
        git.switch(allow_orphan=True)
    # backup targets
    backup_targets = _backup_targets(config, git, logger)
    # commit
    for target in backup_targets:
        logger.info(f"commit {format_timestamp(target.timestamp)}")
        # load backup repository
        if backup is None:
            backup = Backup.load(
                config.scrapbox.project,
                git.path,
                page_order=config.git.page_order,
                logger=logger,
            )
        # commit
        commit_backup(
            config,
            target,
            backup=backup,
            logger=logger,
        )


def commit_backup(
    config: Config,
    data: BackupJSONs,
    *,
    backup: Optional[Backup] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    logger = logger or logging.getLogger(__name__)
    git = config.git.git(logger=logger)
    # git init
    if not git.exists():
        logger.info(f'create git repository "{git.path}"')
        git.init()
    # git switch
    git.switch(allow_orphan=True)
    # initial commit
    _initial_commit(config, git, [data])
    # staging
    commit_target = staging_backup(
        config,
        data,
        backup=backup,
        logger=logger,
    )
    if commit_target is None:
        logger.error("failed to staging")
        return
    # commit message
    message = Commit.message(
        config.scrapbox.project,
        data.timestamp,
        data.load_info(),
    )
    # commit
    git.commit(
        commit_target,
        message,
        timestamp=data.timestamp,
    )


def staging_backup(
    config: Config,
    data: BackupJSONs,
    *,
    backup: Optional[Backup] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[CommitTarget]:
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
            logger=logger,
        )
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
            page_order=config.git.page_order,
        )
        backup.save(logger=logger)
        commit_target = CommitTarget(updated=set(backup.save_files()))
    else:
        # update
        update_diff = backup.update(
            backup_json,
            info_json,
            logger=logger,
        )
        commit_target = CommitTarget(
            added=set(update_diff.added),
            updated=set(update_diff.updated),
            deleted=set(update_diff.removed),
        )
    # external links
    if config.external_link.enabled:
        commit_target.update(
            save_external_links(
                backup,
                git.path,
                config=config.external_link,
                logger=logger,
            )
        )
    return commit_target


def _backup_targets(
    config: Config,
    git: Git,
    logger: logging.Logger,
) -> list[BackupJSONs]:
    # backup start date
    backup_start = (
        int(config.scrapbox.backup_start_date.timestamp())
        if config.scrapbox.backup_start_date is not None
        else None
    )
    # get latest backup timestamp
    latest_commit = git.latest_commit_timestamp()
    logger.info(f"latest backup: {format_timestamp(latest_commit)}")
    # threshold timestamp
    threshold = max(
        (x for x in [backup_start, latest_commit] if x is not None),
        default=None,
    )
    # find backup
    storage = config.scrapbox.save_directory.storage()
    targets = [
        backup
        for backup in storage.backups()
        if threshold is None or threshold < backup.timestamp
    ]
    return targets


def _initial_commit(
    config: Config,
    git: Git,
    backups: list[BackupJSONs],
) -> None:
    # empty initial commit is enabled
    if config.git.empty_initial_commit is None:
        return
    # branch already exists
    if config.git.branch in git.branches():
        return
    # commit
    timestamp = _initial_commit_timestamp(
        config.git.empty_initial_commit,
        backups,
    )
    git.commit(
        CommitTarget(),
        config.git.empty_initial_commit.message,
        timestamp=timestamp,
    )


def _initial_commit_timestamp(
    config: GitEmptyInitialCommitConfig,
    backups: list[BackupJSONs],
) -> int:
    match config.timestamp:
        case datetime.datetime():
            return int(config.timestamp.timestamp())
        case "oldest_backup":
            timestamp = min((backup.timestamp for backup in backups), default=None)
            if timestamp is None:
                raise InitialCommitError(
                    "Since there is no backup, unable to define timestamp"
                )
            return timestamp
        case "oldest_created_page":
            # select oldest data
            backup = min(backups, default=None, key=lambda backup: backup.timestamp)
            if backup is None:
                raise InitialCommitError(
                    "Since there is no backup, unable to define timestamp"
                )
            # load json
            backup_data = backup.load_backup()
            if backup_data is None:
                raise InitialCommitError(
                    f'Could not load oldest backup "{backup.backup_path}"'
                )
            # oldest created page
            timestamp = min(
                (page["created"] for page in backup_data["pages"]),
                default=None,
            )
            if timestamp is None:
                raise InitialCommitError(
                    "There is no page in oldest backup"
                    f" ({format_timestamp(backup.timestamp)})"
                )
            return timestamp
