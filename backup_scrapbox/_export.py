import dataclasses
import json
import logging
import re
from typing import Optional
import jsonschema
from ._env import Env
from ._json import BackupInfoJSON, jsonschema_backup_info, parse_json


def export(
        env: Env,
        logger: logging.Logger) -> None:
    # commits
    commits = _commits(env, logger)


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
        env: Env,
        logger: logging.Logger) -> list[Commit]:
    git = env.git(logger=logger)
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
