# -*- coding: utf-8 -*-

import datetime
import logging
import os
import pathlib
import subprocess
from typing import Optional


def is_git_repository(
        path: pathlib.Path) -> bool:
    return path.is_dir() and path.joinpath('.git').exists()


def git_ls_files(
        repository: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None) -> list[pathlib.Path]:
    logger = logger or logging.getLogger(__name__)
    # ls-files
    command = ['git', 'ls-files', '-z']
    process = git_command(command, repository, logger=logger)
    return [repository.joinpath(path)
            for path in process.stdout.split('\0')
            if path]


def git_commit(
        repository: pathlib.Path,
        message: str,
        *,
        option: Optional[list[str]] = None,
        timestamp: Optional[int] = None,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    # commit
    command = ['git', 'commit', '--message', message]
    env: dict[str, str] = {}
    if option is not None:
        command.extend(option)
    # set: commit date & author date
    if timestamp is not None:
        commit_time = datetime.datetime.fromtimestamp(timestamp)
        env['GIT_AUTHOR_DATE'] = commit_time.isoformat()
        env['GIT_COMMITTER_DATE'] = commit_time.isoformat()
    git_command(command, repository, logger=logger, env=env if env else None)


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
    process = git_command(command, repository, logger=logger)
    timestamp = process.stdout.rstrip('\n')
    if timestamp.isdigit():
        return int(timestamp)
    return None


def git_command(
        command: list[str],
        repository: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None,
        env: Optional[dict[str, str]] = None) -> subprocess.CompletedProcess:
    logger = logger or logging.getLogger(__name__)
    logger.debug('command: %s', command)
    # check if the repository exists
    if not is_git_repository(repository):
        logger.error('git repository "%s" does not exist', repository)
    try:
        process = subprocess.run(
                command,
                check=True,
                cwd=repository,
                env=dict(os.environ, **env) if env is not None else None,
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
