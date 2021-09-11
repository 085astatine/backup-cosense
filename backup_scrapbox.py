#!/usr/bin/env python

import argparse
import logging
from typing import Optional
from dotenv import dotenv_values


def backup_scrapbox(
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
    # main
    backup_scrapbox(logger=logger)
