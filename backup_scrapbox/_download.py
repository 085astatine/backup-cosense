import logging
import time
from typing import Callable, Optional
import requests
from ._env import Env
from ._git import Git
from ._json import (
    BackupJSON, BackupInfoJSON, BackupListJSON, jsonschema_backup,
    jsonschema_backup_list, request_json, save_json)
from ._utility import format_timestamp


def download(
        env: Env,
        logger: logging.Logger,
        request_interval: float) -> None:
    git = env.git(logger=logger)
    with env.session() as session:
        # list
        backup_list = _request_backup_list(env, session, logger)
        time.sleep(request_interval)
        if not backup_list:
            return
        # backup
        for info in filter(_backup_filter(git, logger), backup_list):
            # download
            _download_backup(env, session, info, logger, request_interval)


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
        git: Git,
        logger: logging.Logger) -> Callable[[BackupInfoJSON], bool]:
    # get the latest backup timestamp from the Git repository
    latest_timestamp = git.latest_commit_timestamp()
    logger.info('latest backup: %s', format_timestamp(latest_timestamp))

    def timestamp_filter(backup: BackupInfoJSON) -> bool:
        if latest_timestamp is None:
            return True
        return latest_timestamp < backup['backuped']

    return timestamp_filter


def _download_backup(
        env: Env,
        session: requests.Session,
        info: BackupInfoJSON,
        logger: logging.Logger,
        request_interval: float) -> None:
    # timestamp
    timestamp = info['backuped']
    # path
    storage = env.backup_storage()
    if storage.exists(timestamp):
        logger.debug(
                'skip backup %s: already exists',
                format_timestamp(timestamp))
        return
    # request
    logger.info(
            'download backup %s',
            format_timestamp(timestamp))
    url = f'{_base_url(env)}/{info["id"]}.json'
    backup: Optional[BackupJSON] = request_json(
            url,
            session=session,
            schema=jsonschema_backup(),
            logger=logger)
    time.sleep(request_interval)
    if backup is None:
        return
    # save backup
    backup_path = storage.backup_path(timestamp)
    logger.info('save %s', backup_path)
    save_json(backup_path, backup)
    # save backup info
    info_path = storage.info_path(timestamp)
    logger.info('save %s', info_path)
    save_json(info_path, info)
