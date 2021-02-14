import os
import os.path
from typing import Dict, Tuple, Optional, BinaryIO, Type, TypeVar, Callable, List
from backup_server import datamodel, scanner
import logging
import sys
import yaml
from datetime import datetime
from dataclasses import dataclass, is_dataclass, fields
import bz2
import gzip
import base64

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logging.getLogger("sqlalchemy").level = logging.INFO


def main():
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    config = datamodel.ConfigBackup.parse_obj(config)
    backup(config)


def backup(config: datamodel.ConfigBackup):
    scanner = scanner.Scanner()
    try:
        with COMPRESSION_TYPES[config.cache.compression_type](config.cache.path, "rt") as file:
            aaa = yaml.safe_load(file)
            last_scan = datamodel.Backup.parse_obj(aaa)
    except FileExistsError:
        last_scan = None

    backup_detail = datamodel.Backup(generated=datetime.now())
    for name, path_config in config.paths.items():
        scan_result, children = scanner.scan(path_config.path, last_scan=last_scan.backups.get(name))
        backup_detail.backups[name] = datamodel.FileName(stat=scan_result, children=children)

    with COMPRESSION_TYPES[config.cache.compression_type](config.cache.path, "wt") as file:
        yaml.safe_dump(backup_detail.dict(), file)

    return {backup: base64.b64encode(detail.stat.hash).decode() for backup, detail in backup_detail.backups.items()}


if __name__ == '__main__':
    main()
