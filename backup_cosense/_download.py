import logging
import time
from typing import Any, Callable, Optional, TypedDict

import requests

from ._backup import (
    BackupInfoJSON,
    BackupJSON,
    jsonschema_backup,
    jsonschema_backup_info,
)
from ._config import Config
from ._json import request_json, save_json
from ._utility import format_timestamp


class BackupListJSON(TypedDict):
    backupEnable: bool
    backups: list[BackupInfoJSON]


def jsonschema_backup_list() -> dict[str, Any]:
    schema = {
        "type": "object",
        "required": ["backups"],
        "additionalProperties": False,
        "properties": {
            "backupEnable": {"type": "boolean"},
            "backups": {
                "type": "array",
                "items": jsonschema_backup_info(),
            },
        },
    }
    return schema


def download_backups(
    config: Config,
    *,
    logger: Optional[logging.Logger] = None,
) -> None:
    logger = logger or logging.getLogger(__name__)
    with _session(config) as session:
        # list
        backup_list = _request_backup_list(config, session, logger)
        time.sleep(config.cosense.request_interval)
        if not backup_list:
            return
        # backup
        for info in filter(_backup_filter(config, logger), backup_list):
            # download
            _download_backup(config, session, info, logger)
            time.sleep(config.cosense.request_interval)


def _base_url(config: Config) -> str:
    domain = config.cosense.domain
    project = config.cosense.project
    return f"https://{domain}/api/project-backup/{project}"


def _session(config: Config) -> requests.Session:
    session = requests.Session()
    # cookie
    domains = ["scrapbox.io", "cosen.se"]
    for domain in domains:
        session.cookies.set(
            "connect.sid",
            config.cosense.session_id,
            domain=domain,
        )
    # user agent
    if config.cosense.user_agent is not None:
        session.headers.update({"User-Agent": config.cosense.user_agent.create()})
    return session


def _request_backup_list(
    config: Config,
    session: requests.Session,
    logger: logging.Logger,
) -> list[BackupInfoJSON]:
    # request to .../project-backup/list
    response: Optional[BackupListJSON] = request_json(
        f"{_base_url(config)}/list",
        session=session,
        timeout=config.cosense.request_timeout,
        schema=jsonschema_backup_list(),
        logger=logger,
    )
    # failed to request
    if response is None:
        return []
    # backup info list
    backup_list = response["backups"]
    # backups is empty
    if not backup_list:
        logger.info("there are no backup")
        return []
    # output to logger
    oldest_backup_timestamp = min(info["backuped"] for info in backup_list)
    latest_backup_timestamp = max(info["backuped"] for info in backup_list)
    logger.info(
        f"there are {len(backup_list)} backups:"
        f" {format_timestamp(oldest_backup_timestamp)}"
        f" ~ {format_timestamp(latest_backup_timestamp)}"
    )
    # sort by old...new
    return sorted(backup_list, key=lambda backup: backup["backuped"])


def _backup_filter(
    config: Config,
    logger: logging.Logger,
) -> Callable[[BackupInfoJSON], bool]:
    # backup start date
    start_timestamp = (
        config.cosense.backup_start_date.timestamp()
        if config.cosense.backup_start_date is not None
        else None
    )
    # get the latest backup timestamp from the Git repository
    git = config.git.create(logger=logger)
    latest_timestamp = git.latest_commit_timestamp()
    logger.info(f"latest backup: {format_timestamp(latest_timestamp)}")
    # backup archive
    archive = config.cosense.backup_archive.create(logger=logger)

    def backup_filter(backup: BackupInfoJSON) -> bool:
        timestamp = backup["backuped"]
        if archive.file_path(timestamp).backup.exists():
            logger.debug(f"skip {format_timestamp(timestamp)}: already downloaded")
            return False
        if start_timestamp is not None and start_timestamp > timestamp:
            logger.debug(
                f"skip {format_timestamp(timestamp)}: older than backup start date"
            )
            return False
        if latest_timestamp is None:
            return True
        if timestamp <= latest_timestamp:
            logger.debug(f"skip {format_timestamp(timestamp)}: older than latest")
        return latest_timestamp < timestamp

    return backup_filter


def _download_backup(
    config: Config,
    session: requests.Session,
    info: BackupInfoJSON,
    logger: logging.Logger,
) -> None:
    # timestamp
    timestamp = info["backuped"]
    # request
    logger.info(f"download backup {format_timestamp(timestamp)}")
    url = f'{_base_url(config)}/{info["id"]}.json'
    backup: Optional[BackupJSON] = request_json(
        url,
        session=session,
        timeout=config.cosense.request_timeout,
        schema=jsonschema_backup(),
        logger=logger,
    )
    if backup is None:
        return
    # save
    archive = config.cosense.backup_archive.create(logger=logger)
    file_path = archive.file_path(timestamp)
    # save backup
    logger.info(f'save "{file_path.backup}"')
    save_json(file_path.backup, backup)
    # save backup info
    logger.info(f'save "{file_path.info}"')
    save_json(file_path.info, info)
