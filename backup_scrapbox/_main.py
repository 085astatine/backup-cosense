import argparse
import logging
import sys
import textwrap
from typing import Final, Literal, Optional
from ._env import Env, InvalidEnvError, load_env
from ._download import download
from ._commit import commit


Target = Optional[Literal['download', 'commit']]


_REQUEST_INTERVAL: Final[float] = 3.0


def backup_scrapbox(
        env: Env,
        *,
        target: Target = 'all',
        logger: Optional[logging.Logger] = None,
        request_interval: float = _REQUEST_INTERVAL) -> None:
    logger = logger or logging.getLogger(__name__)
    logger.info('backup-scrapbox')
    # download backup
    if target in (None, 'download'):
        logger.info('target: download')
        download(env, logger, request_interval)
    # commit
    if target in (None, 'commit'):
        logger.info('target: commit')
        commit(env, logger)


def main(
        *,
        args: Optional[list[str]] = None,
        env: Optional[Env] = None,
        logger: Optional[logging.Logger] = None) -> None:
    # logger
    if logger is None:
        logger = logging.getLogger('backup-scrapbox')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.formatter = logging.Formatter(
                fmt='%(asctime)s %(name)s:%(levelname)s:%(message)s')
        logger.addHandler(handler)
    # option
    option = _argument_parser().parse_args(args=args)
    if option.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug('option: %s', option)
    # .env
    if env is None:
        try:
            env = load_env(option.env, logger=logger)
        except InvalidEnvError as error:
            sys.stderr.write(f'invalid env file: {option.env}\n')
            sys.stderr.write('{0}'.format(
                    textwrap.indent(str(error), ' ' * 4)))
            sys.exit(1)
    # main
    backup_scrapbox(
            env,
            target=option.target,
            logger=logger,
            request_interval=option.request_interval)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    # sub parser
    sub_parsers = parser.add_subparsers(
            dest='target',
            help='default: download & commit')
    # default: download & commit
    _add_common_arguments(parser)
    _add_download_arguments(parser)
    # download
    download_parser = sub_parsers.add_parser(
            'download',
            help='download backup from scrapbox.io')
    _add_common_arguments(download_parser)
    _add_download_arguments(download_parser)
    # commit
    commit_parser = sub_parsers.add_parser(
            'commit',
            help='commit to Git repository')
    _add_common_arguments(commit_parser)
    return parser


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    # env
    parser.add_argument(
            '--env',
            dest='env',
            default='.env',
            metavar='DOTENV',
            help='env file (default %(default)s)')
    # verbose
    parser.add_argument(
            '-v', '--verbose',
            dest='verbose',
            action='store_true',
            help='set log level to debug')


def _add_download_arguments(parser: argparse.ArgumentParser) -> None:
    # request interval
    parser.add_argument(
            '--request-interval',
            dest='request_interval',
            type=float,
            default=_REQUEST_INTERVAL,
            metavar='SECONDS',
            help='request interval seconds (default %(default)s)')
