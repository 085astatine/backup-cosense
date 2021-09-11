#!/usr/bin/env python

import argparse
import logging
import sys
from typing import Optional, TypedDict, cast
from dotenv import dotenv_values


class Config(TypedDict):
    project: str
    session_id: str


def backup_scrapbox(
        config: Config,
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    logger.info('backup-scrapbox')


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    # env
    parser.add_argument(
            '--env',
            dest='env',
            default='.env',
            metavar='DOTENV',
            help='env file (default .env)')
    # verbose
    parser.add_argument(
            '-v', '--verbose',
            dest='verbose',
            action='store_true',
            help='set log level to debug')
    return parser


def validate_config(config: dict[str, Optional[str]]) -> None:
    messages: list[str] = []
    keys = ['project', 'session_id']
    messages.extend(
        f'"{key}" is not defined\n'
        for key in keys
        if not (key in config and isinstance(config[key], str)))
    if messages:
        raise Exception(''.join(messages))


if __name__ == '__main__':
    # logger
    logger = logging.getLogger('backup-scrapbox')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.formatter = logging.Formatter(
            fmt='%(asctime)s %(name)s:%(levelname)s:%(message)s')
    logger.addHandler(handler)
    # option
    option = argument_parser().parse_args()
    if option.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug('option: %s', option)
    # .env
    config = dotenv_values(option.env)
    logger.debug('config: %s', config)
    try:
        validate_config(config)
    except Exception as error:
        sys.stderr.write(f'invalid env file: {option.env}\n')
        sys.stderr.write('{0}\n'.format('\n'.join(
                ' ' * 4 + message
                for message in str(error).split('\n')
                if message)))
        sys.exit(1)
    # main
    backup_scrapbox(
            cast(Config, config),
            logger=logger)
