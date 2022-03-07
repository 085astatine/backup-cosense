import dataclasses
import pathlib
import re
from typing import Optional
from ._json import (
        BackupInfoJSON, BackupJSON, jsonschema_backup, jsonschema_backup_info,
        load_json)


class Backup:
    def __init__(
            self,
            project: str,
            directory: pathlib.Path,
            backup: BackupJSON,
            info: Optional[BackupInfoJSON]) -> None:
        self._project = project
        self._directory = directory
        self._backup = backup
        self._info = info

    @property
    def project(self) -> str:
        return self._project

    @property
    def directory(self) -> pathlib.Path:
        return self._directory

    @property
    def timestamp(self) -> int:
        return self._backup['exported']


@dataclasses.dataclass
class DownloadedBackup:
    timestamp: int
    backup_path: pathlib.Path
    info_path: Optional[pathlib.Path]

    def load_backup(self) -> Optional[BackupJSON]:
        return load_json(
                self.backup_path,
                schema=jsonschema_backup())

    def load_info(self) -> Optional[BackupInfoJSON]:
        if self.info_path is None:
            return None
        return load_json(
                self.info_path,
                schema=jsonschema_backup_info())

    def load(
            self,
            project: str,
            directory: pathlib.Path) -> Optional[Backup]:
        backup = self.load_backup()
        info = self.load_info()
        if backup is None:
            return None
        return Backup(
                project,
                directory,
                backup,
                info)


class BackupStorage:
    def __init__(self, directory: pathlib.Path) -> None:
        self._directory = directory

    def backup_path(self, timestamp: int) -> pathlib.Path:
        return self._directory.joinpath(f'{timestamp}.json')

    def info_path(self, timestamp: int) -> pathlib.Path:
        return self._directory.joinpath(f'{timestamp}.info.json')

    def exists(self, timestamp: int) -> bool:
        return self.backup_path(timestamp).exists()

    def backups(self) -> list[DownloadedBackup]:
        backups: list[DownloadedBackup] = []
        for path in self._directory.iterdir():
            # check if the path is file
            if not path.is_file():
                continue
            # check if the filename is '{timestamp}.json'
            filename_match = re.match(
                    r'^(?P<timestamp>\d+)\.json$',
                    path.name)
            if filename_match is None:
                continue
            timestamp = int(filename_match.group('timestamp'))
            # info path
            info_path = self.info_path(timestamp)
            backups.append(DownloadedBackup(
                    timestamp=timestamp,
                    backup_path=path,
                    info_path=info_path if info_path.exists() else None))
        # sort by old...new
        return sorted(backups, key=lambda backup: backup.timestamp)
