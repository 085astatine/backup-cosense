import dataclasses
import logging
import pathlib
from typing import Final, Optional, get_args
import dacite
import dotenv
from ._backup import BackupStorage, PageOrder
from ._git import Git


@dataclasses.dataclass
class Env:
    project: str
    session_id: str
    save_directory: str
    git_repository: str
    git_branch: Optional[str] = None
    page_order: Optional[PageOrder] = None

    def git(
            self,
            *,
            logger: Optional[logging.Logger] = None) -> Git:
        return Git(
                pathlib.Path(self.git_repository),
                branch=self.git_branch,
                logger=logger)

    def backup_storage(self) -> BackupStorage:
        return BackupStorage(pathlib.Path(self.save_directory))


_REQUIRED_KEYS: Final[list[str]] = [
        'project',
        'session_id',
        'save_directory',
        'git_repository']
_OPTIONAL_KEYS: Final[list[str]] = ['git_branch', 'page_order']


class InvalidEnvError(Exception):
    pass


def load_env(
        envfile: str,
        *,
        logger: Optional[logging.Logger] = None) -> Env:
    logger = logger or logging.getLogger(__name__)
    # load
    logger.info(f'load env from "{envfile}"')
    env = dotenv.dotenv_values(envfile)
    logger.debug(f'loaded env: {env}')
    # set optional key
    for key in _OPTIONAL_KEYS:
        if key not in env:
            env[key] = None
        elif env[key] == '':
            env[key] = None
    logger.debug(f'env: {env}')
    # validate
    validate_env(env)
    return dacite.from_dict(data_class=Env, data=env)


def validate_env(env: dict[str, Optional[str]]) -> None:
    messages: list[str] = []
    # required keys
    for key in _REQUIRED_KEYS:
        if key not in env:
            messages.append(f'"{key}" is not defined\n')
        elif not isinstance(env[key], str):
            messages.append(f'"{key}" is not string\n')
    # optional keys
    for key in _OPTIONAL_KEYS:
        if key not in env:
            messages.append(f'"{key}" is not defined\n')
        elif not (env[key] is None or isinstance(env[key], str)):
            messages.append(f'"{key}" is not None or string\n')
    # page order
    if 'page_order' in env:
        page_orders = [None, *get_args(PageOrder)]
        if env['page_order'] not in page_orders:
            messages.append('"page_order" is not {0}\n'.format(
                    ' / '.join(repr(x) for x in page_orders)))
    if messages:
        raise InvalidEnvError(''.join(messages))
