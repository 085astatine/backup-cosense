# -*- coding: utf-8 -*-

import logging
import subprocess
from typing import Optional


def git_show_latest_timestamp(
        repository: str,
        *,
        logger: Optional[logging.Logger] = None) -> Optional[int]:
    logger = logger or logging.getLogger(__name__)
    command = ['git', 'show', '-s', '--format=%ct']
    process = subprocess.run(
            command,
            cwd=repository,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    logger.debug('command: %s', process.args)
    logger.debug('return code: %d', process.returncode)
    if process.returncode != 0:
        logger.error('stderr:\n%s', process.stderr.rstrip('\n'))
        return None
    logger.debug('stdout: %s', process.stdout.rstrip('\n'))
    timestamp = process.stdout.rstrip('\n')
    if timestamp.isdigit():
        return int(timestamp)
    return None
