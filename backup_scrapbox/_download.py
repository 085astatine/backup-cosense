# -*- coding: utf-8 -*-

import json
import logging
import time
from typing import Any, Optional
import requests
from ._env import Env


def download(
        env: Env,
        logger: logging.Logger,
        request_interval: float) -> None:
    url_base = f'https://scrapbox.io/api/project-backup/{env["project"]}'
    # list
    backup_list = _request_json(
            f'{url_base}/list',
            env['session_id'],
            logger)
    print(json.dumps(backup_list, ensure_ascii=False, indent=2))
    time.sleep(request_interval)


def _request_json(
        url: str,
        session_id: str,
        logger: logging.Logger) -> Optional[Any]:
    cookie = {'connect.sid': session_id}
    logger.info('request: %s', url)
    response = requests.get(url, cookies=cookie)
    if not response.ok:
        return None
    return json.loads(response.text)
