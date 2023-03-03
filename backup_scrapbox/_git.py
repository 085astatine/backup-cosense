from __future__ import annotations
import dataclasses
import datetime
import itertools
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import textwrap
from typing import Iterator, Optional
import jsonschema
from ._backup import BackupInfoJSON, jsonschema_backup_info
from ._json import parse_json
from .exceptions import CommitTargetError, GitNotFoundError


@dataclasses.dataclass
class Commit:
    hash: str
    timestamp: int
    body: str

    def time(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.timestamp)

    def backup_info(self) -> Optional[BackupInfoJSON]:
        # check if the body is empty
        if not self.body:
            return None
        # parse as JSON
        body = '{' + ','.join(self.body.split('\n')) + '}'
        try:
            info = parse_json(
                    body,
                    schema=jsonschema_backup_info())
        except json.decoder.JSONDecodeError:
            return None
        except jsonschema.exceptions.ValidationError:
            return None
        return info

    @staticmethod
    def message(
            project: str,
            timestamp: int,
            info: Optional[BackupInfoJSON]) -> str:
        # header
        header = f'{project} {datetime.datetime.fromtimestamp(timestamp)}'
        # body
        body: list[str] = []
        if info is not None:
            body.extend(
                    line.removesuffix(',')
                    for line
                    in textwrap.dedent(
                            json.dumps(info, ensure_ascii=False, indent=2)
                            .removeprefix('{\n')
                            .removesuffix('\n}')).split('\n'))
        # message
        if not body:
            return header
        return '\n'.join([header, '', *body])


@dataclasses.dataclass
class CommitTarget:
    added: set[pathlib.Path] = dataclasses.field(default_factory=set)
    updated: set[pathlib.Path] = dataclasses.field(default_factory=set)
    deleted: set[pathlib.Path] = dataclasses.field(default_factory=set)

    def __post_init__(self) -> None:
        self.normalize()
        self.validate()

    def normalize(self) -> None:
        self.added = {path.resolve() for path in self.added}
        self.updated = {path.resolve() for path in self.updated}
        self.deleted = {path.resolve() for path in self.deleted}

    def validate(self) -> None:
        errors: list[str] = []
        # check intersection
        for set1, set2 in itertools.combinations(
                ['added', 'updated', 'deleted'],
                2):
            for path in getattr(self, set1) & getattr(self, set2):
                errors.append(
                        f'"{path}" exists in both "{set1}" and "{set2}"')
        # raise error
        if errors:
            raise CommitTargetError(''.join(errors))

    def update(self, other: CommitTarget) -> CommitTarget:
        self.added |= other.added
        self.updated |= other.updated
        self.deleted |= other.deleted
        self.validate()
        return self

    def is_empty(self) -> bool:
        return not (self.added or self.updated or self.deleted)


class Git:
    def __init__(
            self,
            path: pathlib.Path,
            *,
            executable: Optional[str] = None,
            branch: Optional[str] = None,
            user_name: Optional[str] = None,
            user_email: Optional[str] = None,
            staging_step_size: int = 1,
            logger: Optional[logging.Logger] = None) -> None:
        self._path = path
        self._executable = (
                executable
                if executable is not None
                else _find_executable())
        self._branch = branch
        self._user_name = user_name
        self._user_email = user_email
        self._staging_step_size = staging_step_size
        self._logger = logger or logging.getLogger(__name__)

    @property
    def path(self) -> pathlib.Path:
        return self._path

    def exists(self) -> bool:
        # check if path exists
        if not self.path.is_dir():
            return False
        # check if path == `git rev-perse --show-toplevel`
        try:
            process = _execute_git_command(
                    [self._executable, 'rev-parse', '--show-toplevel'],
                    self.path,
                    logger=self._logger,
                    ignore_error=True)
            toplevel = pathlib.Path(process.stdout.removesuffix('\n'))
        except subprocess.CalledProcessError:
            return False
        return self.path.resolve() == toplevel

    def switch(
            self,
            *,
            allow_orphan: bool = False) -> None:
        # branch is not selected
        if self._branch is None:
            return
        # current branch
        branch = _execute_git_command(
                [self._executable, 'branch', '--show-current'],
                self.path,
                logger=self._logger).stdout.rstrip('\n')
        if branch == self._branch:
            return
        # switch
        if allow_orphan and self._branch not in self.branches():
            # create orphan branch
            self._logger.info(
                    f'create orphan branch "{self._branch}" before commit')
            _execute_git_command(
                    [self._executable, 'switch', '--orphan', self._branch],
                    self._path,
                    logger=self._logger)
        else:
            self._logger.info(
                    f'switch git branch from "{branch}" to "{self._branch}"')
            _execute_git_command(
                [self._executable, 'switch', self._branch],
                self.path,
                logger=self._logger)

    def execute(
            self,
            command: list[str],
            *,
            ignore_error: bool = False,
            env: Optional[dict[str, str]] = None
    ) -> subprocess.CompletedProcess:
        self._logger.debug(f'command: {command}')
        # check if the repository exists
        if not self.exists():
            self._logger.error(f'git repository "{self.path}" does not exist')
        # switch branch
        self.switch()
        # execute command
        return _execute_git_command(
                command,
                self.path,
                logger=self._logger,
                ignore_error=ignore_error,
                env=env)

    def branches(self) -> list[str]:
        try:
            process = _execute_git_command(
                    [self._executable, 'branch', '--list'],
                    self.path,
                    ignore_error=True,
                    logger=self._logger)
            return [line.lstrip('* ') for line in process.stdout.splitlines()]
        except subprocess.CalledProcessError:
            pass
        return []

    def init(self) -> None:
        # check if Git repository already exists
        if self.exists():
            self._logger.error(
                    f'git repository "{self.path}" already exists')
            return
        # mkdir
        if not self.path.exists():
            self.path.mkdir(parents=True)
        # git init
        command = [self._executable, 'init']
        if self._branch is not None:
            command.extend(['--initial-branch', self._branch])
        _execute_git_command(
            command,
            self.path,
            logger=self._logger)

    def ls_files(self) -> list[pathlib.Path]:
        process = self.execute([self._executable, 'ls-files', '-z'])
        return [self.path.joinpath(path)
                for path in process.stdout.split('\0')
                if path]

    def commit(
            self,
            target: CommitTarget,
            message: str,
            *,
            option: Optional[list[str]] = None,
            timestamp: Optional[int] = None) -> None:
        # create the orphan branch if the target branch does not exist
        self.switch(allow_orphan=True)
        # target
        for added in _into_steps(target.added, self._staging_step_size):
            self.execute([self._executable, 'add', *added])
        for updated in _into_steps(target.updated, self._staging_step_size):
            self.execute([self._executable, 'add', *updated])
        for deleted in _into_steps(target.deleted, self._staging_step_size):
            self.execute([self._executable, 'rm', '--cached', *deleted])
        # command
        command: list[str] = [self._executable]
        if self._user_name is not None:
            command.extend(['-c', f'user.name={self._user_name}'])
        if self._user_email is not None:
            command.extend(['-c', f'user.email={self._user_email}'])
        command.extend(['commit', '--message', message])
        if target.is_empty():
            command.append('--allow-empty')
        if option is not None:
            command.extend(option)
        # set: commit date & author date
        env: dict[str, str] = {}
        if timestamp is not None:
            commit_time = datetime.datetime.fromtimestamp(timestamp)
            env['GIT_AUTHOR_DATE'] = commit_time.isoformat()
            env['GIT_COMMITTER_DATE'] = commit_time.isoformat()
        self.execute(command, env=env if env else None)

    def commits(
            self,
            *,
            option: Optional[list[str]] = None) -> list[Commit]:
        # log format
        log_format = '%n'.join([
                'hash: %H',
                'timestamp: %ct',
                'body:',
                '%b'])
        # git log
        command = [self._executable, 'log', '-z', f'--format={log_format}']
        if option is not None:
            command.extend(option)
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
                self._logger.debug(f'commit: {repr(commit)}')
                commits.append(commit)
            else:
                self._logger.warning(f'failed to parse commit "{repr(log)}"')
        # sort by old...new
        return sorted(commits, key=lambda commit: commit.timestamp)

    def latest_commit_timestamp(self) -> Optional[int]:
        # check if the repository exists
        if not self.exists():
            self._logger.warning(
                    f'git repository "{self.path}" does not exist')
            return None
        # git show -s --format=%ct
        try:
            process = self.execute(
                [self._executable, 'show', '-s', '--format=%ct'],
                ignore_error=True)
        except subprocess.CalledProcessError:
            return None
        timestamp = process.stdout.rstrip('\n')
        if timestamp.isdigit():
            return int(timestamp)
        return None


def _find_executable() -> str:
    executable = shutil.which('git')
    if executable is None:
        raise GitNotFoundError('git executable not found in PATH')
    return executable


def _execute_git_command(
        command: list[str],
        repository: pathlib.Path,
        *,
        logger: Optional[logging.Logger] = None,
        ignore_error: bool = False,
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
        if ignore_error:
            logger.debug(f'{error.__class__.__name__}: {error_info}')
        else:
            logger.error(f'{error.__class__.__name__}: {error_info}')
        raise error
    return process


def _into_steps(
        paths: set[pathlib.Path],
        step_size: int) -> Iterator[list[str]]:
    chunk: list[str] = []
    for path in paths:
        chunk.append(path.as_posix())
        if len(chunk) >= step_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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
