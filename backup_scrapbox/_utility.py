import datetime
from typing import Optional


def format_timestamp(
        timestamp: Optional[int]) -> str:
    if timestamp is None:
        return 'None'
    return f'{datetime.datetime.fromtimestamp(timestamp)} ({timestamp})'
