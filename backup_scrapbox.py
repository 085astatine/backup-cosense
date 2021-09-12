#!/usr/bin/env python

import argparse
import json
import logging
import sys
import time
from typing import Any, Final, Optional, TypedDict, cast
from dotenv import dotenv_values
import requests


REQUEST_INTERVAL: Final[float] = 3.0


class Config(TypedDict):
    project: str
    session_id: str


def backup_scrapbox(
        config: Config,
        *,
        logger: Optional[logging.Logger] = None,
        request_interval: float = REQUEST_INTERVAL) -> None:
    logger = logger or logging.getLogger(__name__)
    logger.info('backup-scrapbox')
    # backup
    backup(config, logger, request_interval)


def backup(
        config: Config,
        logger: logging.Logger,
        request_interval: float) -> None:
    url_base = f'https://scrapbox.io/api/project-backup/{config["project"]}'
    # list
    backup_list = request_json(
            f'{url_base}/list',
            config['session_id'],
            logger)
    print(json.dumps(backup_list, ensure_ascii=False, indent=2))
    time.sleep(request_interval)


def request_json(
        url: str,
        session_id: str,
        logger: logging.Logger) -> Optional[Any]:
    cookie = {'connect.sid': session_id}
    logger.info('request: %s', url)
    response = requests.get(url, cookies=cookie)
    if not response.ok:
        return None
    return json.loads(response.text)


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
    # request interval
    parser.add_argument(
            '--request-interval',
            dest='request_interval',
            type=float,
            default=REQUEST_INTERVAL,
            metavar='SECONDS',
            help=f'request interval seconds (default {REQUEST_INTERVAL})')
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
            logger=logger,
            request_interval=option.request_interval)
