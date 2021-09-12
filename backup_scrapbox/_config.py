# -*- coding: utf-8 -*-

import logging
from typing import Optional, TypedDict, cast
import dotenv


class Config(TypedDict):
    project: str
    session_id: str


def load_config(
        envfile: str,
        *,
        logger: Optional[logging.Logger] = None) -> Config:
    logger = logger or logging.getLogger(__name__)
    # load
    logger.info('load config from "%s"', envfile)
    config = dotenv.dotenv_values(envfile)
    logger.debug('config: %s', config)
    # validate
    validate_config(config)
    return cast(Config, config)


def validate_config(config: dict[str, Optional[str]]) -> None:
    messages: list[str] = []
    keys = ['project', 'session_id']
    messages.extend(
        f'"{key}" is not defined\n'
        for key in keys
        if not (key in config and isinstance(config[key], str)))
    if messages:
        raise Exception(''.join(messages))
