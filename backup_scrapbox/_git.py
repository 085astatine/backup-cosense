import dataclasses
import datetime
import json
import logging
import os
import pathlib
import re
import subprocess
from typing import Optional
import jsonschema
from ._json import BackupInfoJSON, jsonschema_backup_info, parse_json


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


class Git:
    def __init__(
            self,
            path: pathlib.Path,
            *,
            branch: Optional[str] = None,
            logger: Optional[logging.Logger] = None) -> None:
        self._path = path
        self._branch = branch
        self._logger = logger or logging.getLogger(__name__)

    @property
    def path(self) -> pathlib.Path:
        return self._path

    def exists(self) -> bool:
        return self.path.is_dir() and self.path.joinpath('.git').is_dir()

    def execute(
            self,
            command: list[str],
            *,
            env: Optional[dict[str, str]] = None
    ) -> subprocess.CompletedProcess:
        self._logger.debug('command: %s', command)
        # check if the repository exists
        if not self.exists():
            self._logger.error('git repository "%s" does not exist', self.path)
        # switch branch
        if self._branch is not None:
            branch = _execute_git_command(
                    ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                    self.path,
                    logger=self._logger).stdout.rstrip('\n')
            if branch != self._branch:
                self._logger.info(
                        'switch git branch from "%s" to "%s"',
                        branch,
                        self._branch)
                _execute_git_command(
                        ['git', 'switch', self._branch],
                        self.path,
                        logger=self._logger)
        return _execute_git_command(
                command,
                self.path,
                logger=self._logger,
                env=env)

    def ls_files(self) -> list[pathlib.Path]:
        process = self.execute(['git', 'ls-files', '-z'])
        return [self.path.joinpath(path)
                for path in process.stdout.split('\0')
                if path]

    def commit(
            self,
            message: str,
            *,
            option: Optional[list[str]] = None,
            timestamp: Optional[int] = None) -> None:
        # commit
        command = ['git', 'commit', '--message', message]
        if option is not None:
            command.extend(option)
        # set: commit date & author date
        env: dict[str, str] = {}
        if timestamp is not None:
            commit_time = datetime.datetime.fromtimestamp(timestamp)
            env['GIT_AUTHOR_DATE'] = commit_time.isoformat()
            env['GIT_COMMITTER_DATE'] = commit_time.isoformat()
        self.execute(command, env=env if env else None)

    def commits(self) -> list[Commit]:
        # log format
        log_format = '\n'.join([
                'hash: %H',
                'timestamp: %ct',
                'body:',
                '%b'])
        # git log
        command = ['git', 'log', '-z', f'--format={log_format}']
        process = self.execute(command)
        # parse
        commits: list[Commit] = []
        for log in process.stdout.split('\0'):
            # skip empty log
            if not log:
                continue
            # parse log as commit
            commit = _log_to_commit(log)
            if commit is not None:
                self._logger.debug('commit: %s', repr(commit))
                commits.append(commit)
            else:
                self._logger.warning('failed to parse commit "%s"', repr(log))
        # sort by old...new
        return sorted(commits, key=lambda commit: commit.timestamp)

    def latest_commit_timestamp(self) -> Optional[int]:
        # check if the repository exists
        if not self.exists():
            self._logger.warning(
                    'git repository "%s" does not exist',
                    self.path)
            return None
        # git show -s --format=%ct
        process = self.execute(['git', 'show', '-s', '--format=%ct'])
        timestamp = process.stdout.rstrip('\n')
        if timestamp.isdigit():
            return int(timestamp)
        return None


def _execute_git_command(
        command: list[str],
        repository: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None,
        env: Optional[dict[str, str]] = None) -> subprocess.CompletedProcess:
    logger = logger or logging.getLogger(__name__)
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
    return process


def _log_to_commit(log: str) -> Optional[Commit]:
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
