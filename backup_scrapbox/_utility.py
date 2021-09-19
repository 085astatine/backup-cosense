# -*- coding: utf-8 -*-

import datetime
from typing import Optional


def format_timestamp(
        timestamp: Optional[int]) -> str:
    if timestamp is None:
        return 'None'
    return "{0} ({1})".format(
            datetime.datetime.fromtimestamp(timestamp),
            timestamp)
