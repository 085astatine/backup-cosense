import datetime
import logging
import os
import pathlib
import subprocess
from typing import Optional


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
    logger.debug('stdout: %s', repr(process.stdout))
    return process
