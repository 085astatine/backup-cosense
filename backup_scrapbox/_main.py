import argparse
import logging
import pathlib
from typing import Optional
from ._config import Config, load_config
from ._download import download_backups
from ._commit import commit_backups
from ._export import export_backups


def backup_scrapbox(
        *,
        args: Optional[list[str]] = None,
        config: Optional[Config] = None,
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
    logger.debug(f'option: {option}')
    # config TOML
    if config is None:
        config = load_config(option.config, logger=logger)
    # main
    logger.info('backup-scrapbox')
    # download backup
    if option.target in (None, 'download'):
        logger.info('target: download')
        download_backups(config, logger=logger)
    # commit
    if option.target in (None, 'commit'):
        logger.info('target: commit')
        commit_backups(config, logger=logger)
    # export
    if option.target == 'export':
        logger.info('target: export')
        export_backups(config, option.destination, logger=logger)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=__package__)
    _add_common_arguments(parser)
    # sub parser
    sub_parsers = parser.add_subparsers(
            dest='target',
            help='default: download & commit')
    # download
    sub_parsers.add_parser(
            'download',
            help='download backup from scrapbox.io')
    # commit
    sub_parsers.add_parser(
            'commit',
            help='commit to Git repository')
    # export
    export_parser = sub_parsers.add_parser(
            'export',
            help='export backups from Git repository')
    _add_export_arguments(export_parser)
    return parser


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    # config
    parser.add_argument(
            '--config',
            dest='config',
            default='config.toml',
            metavar='TOML',
            type=pathlib.Path,
            help='.toml file (default %(default)s)')
    # verbose
    parser.add_argument(
            '-v', '--verbose',
            dest='verbose',
            action='store_true',
            help='set log level to debug')


def _add_export_arguments(parser: argparse.ArgumentParser) -> None:
    # destination
    parser.add_argument(
            '-d', '--destination',
            dest='destination',
            type=pathlib.Path,
            required=True,
            metavar='DIR',
            help='directory to export backups')
