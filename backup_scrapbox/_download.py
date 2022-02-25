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
    backup_list: Optional[BackupListJSON] = request_json(
            f'{_base_url(env)}/list',
            env.session_id,
            schema=jsonschema_backup_list(),
            logger=logger)
    if backup_list is None:
        return
    if not backup_list['backups']:
        logger.info('there are no backup')
        return
    logger.info(
            'there are %d backups: %s ~ %s',
            len(backup_list['backups']),
            format_timestamp(
                    min(x['backuped'] for x in backup_list['backups'])),
            format_timestamp(
                    max(x['backuped'] for x in backup_list['backups'])))
    time.sleep(request_interval)
    # get the latest backup timestamp from the Git repository
    latest_timestamp = git.latest_commit_timestamp()
    logger.info('latest backup: %s', format_timestamp(latest_timestamp))
    # backup
    for info in sorted(backup_list['backups'], key=lambda x: x['backuped']):
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
