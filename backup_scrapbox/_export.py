import dataclasses
import json
import logging
import pathlib
import subprocess
import re
from typing import Optional
import jsonschema
from ._env import Env
from ._git import Git
from ._json import (
        BackupJSON, BackupInfoJSON, jsonschema_backup,
        jsonschema_backup_info, parse_json, save_json)
from ._utility import format_timestamp


def export(
        env: Env,
        destination: pathlib.Path,
        logger: logging.Logger) -> None:
    git = env.git(logger=logger)
    # check if the destination exists
    if not destination.exists():
        logger.error(
                f'export directory "{destination.as_posix()}" does not exist')
        return
    # commits
    commits = _commits(git, logger)
    if commits:
        logger.info(
                f'{len(commits)} commits:'
                f' {format_timestamp(commits[0].timestamp)}'
                f' ~ {format_timestamp(commits[-1].timestamp)}')
    else:
        logger.info('there are no commits')
    # export
    for commit in commits:
        logger.info(f'export {format_timestamp(commit.timestamp)}')
        _export(env.project,
                git,
                commit,
                destination,
                logger)


@dataclasses.dataclass
class Commit:
    hash: str
    timestamp: int
    body: str

    def backup_info(self) -> Optional[BackupInfoJSON]:
        # check if the body is empty
        if not self.body:
            return None
        # parse as JSON
        body = '{' + ','.join(self.body.split('\n')).replace('\'', '"') + '}'
        try:
            info = parse_json(
                    body,
                    schema=jsonschema_backup_info())
        except json.decoder.JSONDecodeError:
            return None
        except jsonschema.exceptions.ValidationError:
            return None
        return info


def _to_commit(log: str) -> Optional[Commit]:
    commit_match = re.match(
            r'hash: (?P<hash>[0-9a-f]{40})\n'
            r'timestamp: (?P<timestamp>\d+)\n'
            r'body:\n(?P<body>.*?)\n?$',
            log,
            re.DOTALL)
    if commit_match is None:
        return None
    return Commit(
            hash=commit_match.group('hash'),
            timestamp=int(commit_match.group('timestamp')),
            body=commit_match.group('body'))


def _commits(
        git: Git,
        logger: logging.Logger) -> list[Commit]:
    # log format
    log_format = '\n'.join([
            'hash: %H',
            'timestamp: %ct',
            'body:',
            '%b'])
    # git log
    command = ['git', 'log', '-z', f'--format={log_format}']
    process = git.execute(command)
    # parse
    commits: list[Commit] = []
    for log in process.stdout.split('\0'):
        # skip empty string
        if not log:
            continue
        # parse
        commit = _to_commit(log)
        if commit is not None:
            logger.debug('commit: %s', repr(commit))
            commits.append(commit)
        else:
            logger.warning('failed to parse commit "%s"', repr(log))
    # sort by old...new
    return sorted(commits, key=lambda commit: commit.timestamp)


def _export(
        project: str,
        git: Git,
        commit: Commit,
        destination: pathlib.Path,
        logger: logging.Logger) -> None:
    # get backup.json
    command = ['git', 'show', '-z', f'{commit.hash}:{project}.json']
    try:
        process = git.execute(command)
    except subprocess.CalledProcessError:
        logger.warning('skip commit: %s', commit.hash)
        return
    backup_json: Optional[BackupJSON] = parse_json(
            process.stdout,
            schema=jsonschema_backup())
    # save backup.json
    backup_json_path = destination.joinpath(f'{commit.timestamp}.json')
    save_json(backup_json_path, backup_json)
    logger.debug('save %s', backup_json_path)
    # save backup.info.json
    info_json_path = destination.joinpath(f'{commit.timestamp}.info.json')
    info_json = commit.backup_info()
    if info_json is not None:
        save_json(info_json_path, info_json)
