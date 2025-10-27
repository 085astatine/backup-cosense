import argparse
import dataclasses
import logging
import pathlib
from typing import Optional

from ._backup import BackupArchive
from ._commit import commit_backups
from ._config import Config, load_config
from ._download import download_backups
from ._export import export_backups


def backup_cosense(
    *,
    args: Optional[list[str]] = None,
    config: Optional[Config] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    # logger
    if logger is None:
        logger = logging.getLogger("backup-cosense")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.formatter = logging.Formatter(
            fmt="%(asctime)s %(name)s:%(levelname)s:%(message)s"
        )
        logger.addHandler(handler)
    # option
    option = parse_args(args)
    if option.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"option: {option}")
    # config TOML
    if config is None:
        config = load_config(option.config, logger=logger)
    # command
    logger.info("backup-cosense")
    match option:
        case DownloadOption():
            # download backup
            logger.info("command: download")
            download_backups(config, logger=logger)
        case CommitOption():
            # commit
            logger.info("command: commit")
            commit_backups(config, logger=logger)
        case ExportOption():
            # export
            logger.info("command: export")
            destination = BackupArchive(
                option.destination,
                subdirectory=option.subdirectory,
                logger=logger,
            )
            export_backups(config, destination, logger=logger)


@dataclasses.dataclass(frozen=True)
class CommonOption:
    config: pathlib.Path
    verbose: bool

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.set_defaults(cls=cls)

    @classmethod
    def add_common_arguments(cls, parser: argparse.ArgumentParser) -> None:
        # config
        parser.add_argument(
            "--config",
            dest="config",
            default="config.toml",
            metavar="TOML",
            type=pathlib.Path,
            help=".toml file (default %(default)s)",
        )
        # verbose
        parser.add_argument(
            "-v",
            "--verbose",
            dest="verbose",
            action="store_true",
            help="set log level to debug",
        )


@dataclasses.dataclass(frozen=True)
class DownloadOption(CommonOption):
    pass


@dataclasses.dataclass(frozen=True)
class CommitOption(CommonOption):
    pass


@dataclasses.dataclass(frozen=True)
class ExportOption(CommonOption):
    destination: pathlib.Path
    subdirectory: bool
    after: Optional[str]
    before: Optional[str]

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        super().add_arguments(parser)
        # destination
        parser.add_argument(
            "-d",
            "--destination",
            dest="destination",
            required=True,
            metavar="DIR",
            type=pathlib.Path,
            help="directory to export backups",
        )
        # subdirectory
        parser.add_argument(
            "--subdirectory",
            dest="subdirectory",
            action="store_true",
            help="create subdirectories on export",
        )
        # after / since
        parser.add_argument(
            "--after",
            "--since",
            dest="after",
            metavar="<date>",
            help="export commits more recent then <date>",
        )
        # before / until
        parser.add_argument(
            "--before",
            "--untill",
            dest="before",
            metavar="<date>",
            help="export commits older than <date>",
        )


type Option = DownloadOption | CommitOption | ExportOption


def parse_args(args: Optional[list[str]] = None) -> Option:
    parser = _argument_parser()
    option = vars(parser.parse_args(args))
    cls = option.pop("cls")
    return cls(**option)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=__package__)
    # common
    CommonOption.add_common_arguments(parser)
    # sub parser
    sub_parsers = parser.add_subparsers(
        title="command",
        description="command to be executed",
        required=True,
    )
    # download
    download_parser = sub_parsers.add_parser(
        "download",
        help="download backup from scrapbox.io",
    )
    DownloadOption.add_arguments(download_parser)
    # commit
    commit_parser = sub_parsers.add_parser(
        "commit",
        help="commit to Git repository",
    )
    CommitOption.add_arguments(commit_parser)
    # export
    export_parser = sub_parsers.add_parser(
        "export",
        help="export backups from Git repository",
    )
    ExportOption.add_arguments(export_parser)
    return parser
