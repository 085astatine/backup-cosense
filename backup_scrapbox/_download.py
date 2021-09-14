# -*- coding: utf-8 -*-

import datetime
import json
import logging
import pathlib
import time
from typing import Any, Optional
import jsonschema
import requests
from ._env import Env
from ._json import (
    BackupJSON, BackupInfoJSON, BackupListJSON, jsonschema_backup,
    jsonschema_backup_list, save_json)


def download(
        env: Env,
        logger: logging.Logger,
        request_interval: float) -> None:
    url_base = f'https://scrapbox.io/api/project-backup/{env["project"]}'
    # list
    backup_list: Optional[BackupListJSON] = _request_json(
            f'{url_base}/list',
            env['session_id'],
            logger,
            schema=jsonschema_backup_list())
    if backup_list is None:
        return
    logger.debug(
            'response:\n%s',
            json.dumps(backup_list, ensure_ascii=False, indent=2))
    time.sleep(request_interval)
    # TODO: get the timestamp of latest backup
    # backup
    for info in sorted(backup_list['backups'], key=lambda x: x['backuped']):
        # TODO: check whether or not it is a target
        _download_backup(env, info, logger, request_interval)


def _download_backup(
        env: Env,
        info: BackupInfoJSON,
        logger: logging.Logger,
        request_interval: float) -> None:
    # timestamp
    timestamp = info['backuped']
    logger.info(
            'download the backup created at %s (%d)',
            datetime.datetime.fromtimestamp(timestamp),
            timestamp)
    # request
    url = (f'https://scrapbox.io/api/project-backup'
           f'/{env["project"]}/{info["id"]}.json')
    backup: Optional[BackupJSON] = _request_json(
            url,
            env['session_id'],
            logger,
            schema=jsonschema_backup())
    time.sleep(request_interval)
    if backup is None:
        return
    # save
    save_directory = pathlib.Path(env['save_directory'])
    # save backup
    backup_path = save_directory.joinpath(f'{timestamp}.json')
    logger.info('save %s', backup_path)
    save_json(backup_path, backup)
    # save backup info
    info_path = save_directory.joinpath(f'{timestamp}.info.json')
    logger.info('save %s', info_path)
    save_json(info_path, info)


def _request_json(
        url: str,
        session_id: str,
        logger: logging.Logger,
        schema: Optional[dict] = None) -> Optional[Any]:
    cookie = {'connect.sid': session_id}
    logger.info('get request: %s', url)
    response = requests.get(url, cookies=cookie)
    if not response.ok:
        logger.error('failed to get request "%s"', url)
        return None
    # jsonschema validation
    result = json.loads(response.text)
    if schema is not None:
        jsonschema.validate(
                instance=result,
                schema=schema)
    return result
