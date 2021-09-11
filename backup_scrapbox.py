#!/usr/bin/env python

import logging
from typing import Optional


def backup_scrapbox(
        *,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    logger.info('backup-scrapbox')


if __name__ == '__main__':
    # logger
    logger = logging.getLogger('backup-scrapbox')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.formatter = logging.Formatter(
            fmt='%(asctime)s %(name)s:%(levelname)s:%(message)s')
    logger.addHandler(handler)
    # main
    backup_scrapbox(logger=logger)
