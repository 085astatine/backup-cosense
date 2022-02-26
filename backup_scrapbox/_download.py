import logging
import pathlib
import time
from typing import Optional
from ._env import Env
from ._json import (
    BackupJSON, BackupInfoJSON, BackupListJSON, jsonschema_backup,
    jsonschema_backup_list, request_json, save_json)
from ._utility import format_timestamp


def download(
        env: Env,
        logger: logging.Logger,
        request_interval: float) -> None:
    git = env.git(logger=logger)
    # list
    backup_list = _request_backup_list(env, logger)
    time.sleep(request_interval)
    if not backup_list:
        return
    # get the latest backup timestamp from the Git repository
    latest_timestamp = git.latest_commit_timestamp()
    logger.info('latest backup: %s', format_timestamp(latest_timestamp))
    # backup
    for info in backup_list:
        # check whether or not it is a target
        if (latest_timestamp is not None
                and info['backuped'] <= latest_timestamp):
            logger.info(
                    'skip backup %s: older than latest',
                    format_timestamp(info['backuped']))
            continue
        # download
        _download_backup(env, info, logger, request_interval)


def _base_url(env: Env) -> str:
    return f'https://scrapbox.io/api/project-backup/{env.project}'


def _request_backup_list(
        env: Env,
        logger: logging.Logger) -> list[BackupInfoJSON]:
    # request to .../project-backup/list
    response: Optional[BackupListJSON] = request_json(
            f'{_base_url(env)}/list',
            env.session_id,
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


def _download_backup(
        env: Env,
        info: BackupInfoJSON,
        logger: logging.Logger,
        request_interval: float) -> None:
    # timestamp
    timestamp = info['backuped']
    # path
    save_directory = pathlib.Path(env.save_directory)
    backup_path = save_directory.joinpath(f'{timestamp}.json')
    info_path = save_directory.joinpath(f'{timestamp}.info.json')
    if backup_path.exists() and info_path.exists():
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
            env.session_id,
            schema=jsonschema_backup(),
            logger=logger)
    time.sleep(request_interval)
    if backup is None:
        return
    # save backup
    logger.info('save %s', backup_path)
    save_json(backup_path, backup)
    # save backup info
    logger.info('save %s', info_path)
    save_json(info_path, info)
