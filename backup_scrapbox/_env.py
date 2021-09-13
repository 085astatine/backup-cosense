# -*- coding: utf-8 -*-

import logging
from typing import Optional, TypedDict, cast
import dotenv


class Env(TypedDict):
    project: str
    session_id: str


def load_env(
        envfile: str,
        *,
        logger: Optional[logging.Logger] = None) -> Env:
    logger = logger or logging.getLogger(__name__)
    # load
    logger.info('load env from "%s"', envfile)
    env = dotenv.dotenv_values(envfile)
    logger.debug('env: %s', env)
    # validate
    validate_env(env)
    return cast(Env, env)


def validate_env(env: dict[str, Optional[str]]) -> None:
    messages: list[str] = []
    keys = ['project', 'session_id']
    messages.extend(
        f'"{key}" is not defined\n'
        for key in keys
        if not (key in env and isinstance(env[key], str)))
    if messages:
        raise Exception(''.join(messages))
