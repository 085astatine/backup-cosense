import logging
import time
from typing import Callable, Optional
import requests
from ._env import Env
from ._json import (
    BackupJSON, BackupInfoJSON, BackupListJSON, jsonschema_backup,
    jsonschema_backup_list, request_json, save_json)
from ._utility import format_timestamp


def download(
        env: Env,
        logger: logging.Logger,
        request_interval: float) -> None:
    with env.session() as session:
        # list
        backup_list = _request_backup_list(env, session, logger)
        time.sleep(request_interval)
        if not backup_list:
            return
        # backup
        for info in filter(_backup_filter(env, logger), backup_list):
            # download
            _download_backup(env, session, info, logger)
            time.sleep(request_interval)


def _base_url(env: Env) -> str:
    return f'https://scrapbox.io/api/project-backup/{env.project}'


def _request_backup_list(
        env: Env,
        session: requests.Session,
        logger: logging.Logger) -> list[BackupInfoJSON]:
    # request to .../project-backup/list
    response: Optional[BackupListJSON] = request_json(
            f'{_base_url(env)}/list',
            session=session,
            schema=jsonschema_backup_list(),
            logger=logger)
    # failed to request
    if response is None:
        return []
    # backup info list
    backup_list = response['backups']
    # backups is empty
    if not backup_list:
        logger.info('there are no backup')
        return []
    # output to logger
    oldest_backup_timestamp = min(info['backuped'] for info in backup_list)
    latest_backup_timestamp = max(info['backuped'] for info in backup_list)
    logger.info(f'there are {len(backup_list)} backups:'
                f' {format_timestamp(oldest_backup_timestamp)}'
                f' ~ {format_timestamp(latest_backup_timestamp)}')
    # sort by old...new
    return sorted(backup_list, key=lambda backup: backup['backuped'])


def _backup_filter(
        env: Env,
        logger: logging.Logger) -> Callable[[BackupInfoJSON], bool]:
    # get the latest backup timestamp from the Git repository
    git = env.git(logger=logger)
    latest_timestamp = git.latest_commit_timestamp()
    logger.info(f'latest backup: {format_timestamp(latest_timestamp)}')
    # backup storage
    storage = env.backup_storage()

    def backup_filter(backup: BackupInfoJSON) -> bool:
        timestamp = backup['backuped']
        if storage.exists(timestamp):
            logger.debug(
                    f'skip {format_timestamp(timestamp)}: already downloaded')
            return False
        if latest_timestamp is None:
            return True
        if timestamp <= latest_timestamp:
            logger.debug(
                    f'skip {format_timestamp(timestamp)}: older than latest')
        return latest_timestamp < timestamp

    return backup_filter


def _download_backup(
        env: Env,
        session: requests.Session,
        info: BackupInfoJSON,
        logger: logging.Logger) -> None:
    # timestamp
    timestamp = info['backuped']
    # request
    logger.info(
            f'download backup {format_timestamp(timestamp)}')
    url = f'{_base_url(env)}/{info["id"]}.json'
    backup: Optional[BackupJSON] = request_json(
            url,
            session=session,
            schema=jsonschema_backup(),
            logger=logger)
    if backup is None:
        return
    # save
    storage = env.backup_storage()
    # save backup
    backup_path = storage.backup_path(timestamp)
    logger.info(f'save "{backup_path}"')
    save_json(backup_path, backup)
    # save backup info
    info_path = storage.info_path(timestamp)
    logger.info(f'save "{info_path}"')
    save_json(info_path, info)
