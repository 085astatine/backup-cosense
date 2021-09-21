# -*- coding: utf-8 -*-

import logging
from typing import Final, Literal, Optional, TypedDict, cast
import dotenv


PageOrder = Literal['as-is', 'created-asc', 'created-desc']
_PAGE_ORDERS: Final[list[Optional[str]]] = [
        None,
        'as-is',
        'created-asc',
        'created-desc']


class Env(TypedDict):
    project: str
    session_id: str
    save_directory: str
    git_repository: str
    git_branch: Optional[str]
    page_order: Optional[PageOrder]


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
    logger.info('load env from "%s"', envfile)
    env = dotenv.dotenv_values(envfile)
    logger.debug('loaded env: %s', env)
    # set optional key
    for key in _OPTIONAL_KEYS:
        if key not in env:
            env[key] = None
        elif env[key] == '':
            env[key] = None
    logger.debug('env: %s', env)
    # validate
    validate_env(env)
    return cast(Env, env)


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
        if env['page_order'] not in _PAGE_ORDERS:
            messages.append('"page_order" is not {0}\n'.format(
                    ' / '.join(repr(x) for x in _PAGE_ORDERS)))
    if messages:
        raise InvalidEnvError(''.join(messages))
