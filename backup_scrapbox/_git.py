# -*- coding: utf-8 -*-

import logging
import pathlib
import subprocess
from typing import Optional


def is_git_repository(
        path: pathlib.Path) -> bool:
    return path.is_dir() and path.joinpath('.git').exists()


def git_commit(
        repository: pathlib.Path,
        message: str,
        *,
        option: Optional[list[str]] = None,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    # check if the repository exists
    if not is_git_repository(repository):
        logger.warning('git repository "%s" does not exist', repository)
        return None
    # commit
    command = ['git', 'commit', '--message', message]
    if option is not None:
        command.extend(option)
    _run(command, repository, logger)


def git_show_latest_timestamp(
        repository: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None) -> Optional[int]:
    logger = logger or logging.getLogger(__name__)
    # check if the repository exists
    if not is_git_repository(repository):
        logger.warning('git repository "%s" does not exist', repository)
        return None
    # git show -s --format=%ct
    command = ['git', 'show', '-s', '--format=%ct']
    process = _run(command, repository, logger)
    timestamp = process.stdout.rstrip('\n')
    if timestamp.isdigit():
        return int(timestamp)
    return None


def _run(
        command: list[str],
        cwd: pathlib.Path,
        logger: logging.Logger) -> subprocess.CompletedProcess:
    logger.info('command: %s', command)
    try:
        process = subprocess.run(
                command,
                check=True,
                cwd=cwd,
                encoding='utf-8',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as error:
        error_info = {
                'return_code': error.returncode,
                'command': error.cmd,
                'stdout': error.stdout,
                'stderr': error.stderr}
        logger.error('%s: %s', error.__class__.__name__, error_info)
        raise error
    logger.debug('stdout: %s', repr(process.stdout))
    return process
