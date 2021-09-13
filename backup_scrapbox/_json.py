# -*- coding: utf-8 -*-

import json
import pathlib
from typing import Any, Optional, TypedDict


class BackupInfoJSON(TypedDict):
    id: str
    backuped: int
    totalPages: int
    totalLinks: int


def jsonschema_backup_info():
    schema = {
      'type': 'object',
      'required': ['id', 'backuped', 'totalPages', 'totalLinks'],
      'additionalProperties': False,
      'properties': {
        'id': {'type': 'string'},
        'backuped': {'type': 'integer'},
        'totalPages': {'type': 'integer'},
        'totalLinks': {'type': 'integer'},
      },
    }
    return schema


class BackupListJSON(TypedDict):
    backupEnable: bool
    backups: list[BackupInfoJSON]


def jsonschema_backup_list():
    schema = {
      'type': 'object',
      'required': ['backupEnable', 'backups'],
      'additionalProperties': False,
      'properties': {
        'backupEnable': {'type': 'boolean'},
        'backups': {
          'type': 'array',
          'items': jsonschema_backup_info(),
        },
      },
    }
    return schema


class BackupPageJSON(TypedDict):
    title: str
    created: int
    updated: int
    id: str
    lines: list[str]
    linksLc: list[str]


def jsonschema_backup_page():
    schema = {
      'type': 'object',
      'required': ['title', 'created', 'updated', 'id', 'lines', 'linksLc'],
      'additionalProperties': False,
      'properties': {
        'title': {'type': 'string'},
        'created': {'type': 'integer'},
        'updated': {'type': 'integer'},
        'id': {'type': 'string'},
        'lines': {
          'type': 'array',
          'items': {'type': 'string'},
        },
        'linksLc': {
          'type': 'array',
          'items': {'type': 'string'},
        },
      },
    }
    return schema


class BackupJSON(TypedDict):
    name: str
    displayName: str
    exported: int
    pages: list[BackupPageJSON]


def jsonschema_backup():
    schema = {
      'type': 'object',
      'required': ['name', 'displayName', 'exported', 'pages'],
      'additionalProperties': False,
      'properties': {
        'name': {'type': 'string'},
        'displayName': {'type': 'string'},
        'exported': {'type': 'integer'},
        'pages': {
          'type': 'array',
          'items': jsonschema_backup_page(),
        },
      },
    }
    return schema


def load_json(
        path: pathlib.Path) -> Optional[Any]:
    if not path.exists():
        return None
    with path.open() as file:
        return json.load(file)


def save_json(
        path: pathlib.Path,
        data: Any) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    with path.open(mode='w') as file:
        json.dump(
                data,
                file,
                ensure_ascii=False,
                indent=2)
        file.write('\n')
