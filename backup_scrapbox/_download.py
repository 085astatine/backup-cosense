# -*- coding: utf-8 -*-

import json
import logging
import time
from typing import Any, Optional
import jsonschema
import requests
from ._env import Env
from ._json import BackupListJSON, jsonschema_backup_list


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
