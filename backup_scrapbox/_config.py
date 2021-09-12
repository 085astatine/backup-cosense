# -*- coding: utf-8 -*-

from typing import Optional, TypedDict


class Config(TypedDict):
    project: str
    session_id: str


def validate_config(config: dict[str, Optional[str]]) -> None:
    messages: list[str] = []
    keys = ['project', 'session_id']
    messages.extend(
        f'"{key}" is not defined\n'
        for key in keys
        if not (key in config and isinstance(config[key], str)))
    if messages:
        raise Exception(''.join(messages))
