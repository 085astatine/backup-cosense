# -*- coding: utf-8 -*-

from typing import TypedDict


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
