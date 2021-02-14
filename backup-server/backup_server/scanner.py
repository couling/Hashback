from typing import Dict, Tuple, Optional, NamedTuple
import os
import io
import re
import stat
import subprocess
from . import datamodel
import logging


log = logging.getLogger(__name__)


__all__ = ['Scanner']


class Scanner:
    all_files: Dict[Tuple[int, int], datamodel.Inode]

    def __init__(self, mount_points: Dict[int, MountPoint] = None):
        if mount_points is None:
            self.mount_points = self.get_mount_points()
        else:
            self.mount_points = mount_points.copy()
        self.directories = {}
        self.all_files = {}

    def scan(self, path: str, last_scan: Optional[FileName] = None) -> FileName:
        return self._scan(path, last_scan=last_scan)

    def _scan(self, path: str, reference: Optional[Tuple[int, int]] = None,
              last_scan: Optional[FileName] = None) -> FileName:
        if reference is not None and reference in self.all_files:
            return FileName(self.all_files[reference], None)
        s = os.lstat(path)
        reference = (s.st_dev, s.st_ino)
        if reference in self.all_files:
            return FileName(self.all_files[reference], None)
        if stat.S_ISREG(s.st_mode):
            if last_scan is not None:
                result = datamodel.Inode.from_stat(s, last_scan.stat.hash)
                if result == last_scan.stat:
                    return FileName(result, None)
            with open(path, "rb") as file:
                hash_value = datamodel.hash_content(file)
            children = None
        elif stat.S_ISDIR(s.st_mode):
            children = self._list_directory(path, s.st_dev, last_scan)
            child_inodes = {name: child.stat for name, child in children.items()}
            hash_value = datamodel.hash_content(datamodel.dump_dir(child_inodes))
        else:
            raise ValueError(f"Bad file Type {path}")
        result = datamodel.Inode.from_stat(s, hash_value)
        self.all_files[reference] = result
        return FileName(result, children)

    def _list_directory(self, dir_path: str, dir_device: int, last_scan: Optional[FileName] = None
                        ) -> Dict[str, FileName]:
        log.debug(f"Listing {dir_path}")
        entry: os.DirEntry
        dir_contents: Dict[str, FileName] = {}
        try:
            reader = os.scandir(path=dir_path)
        except PermissionError:
            pass
        else:
            with reader:
                for entry in reader:
                    if last_scan is None:
                        last_child = None
                    else:
                        last_child = last_scan.children.get(entry.name)
                    child_data, grandchildren = self._scan(entry.path, (dir_device, entry.inode()), last_child)
                    dir_contents[entry.name] = FileName(stat=child_data, children=grandchildren)
        return dir_contents

    @staticmethod
    def get_mount_points() -> Dict[int, MountPoint]:
        parser = re.compile(r"(.*) on (.*) (type )?\((?P<type>.*)\)")
        p = subprocess.Popen("mount", stdout=subprocess.PIPE)
        result = {}
        with p.stdout:
            reader = io.TextIOWrapper(p.stdout)
            for line in reader:
                source, target, _, _ = parser.match(line).groups()
                s = os.stat(target)
                id = s.st_dev
                result[id] = MountPoint(
                    id=id,
                    source=source,
                    target=target,
                )
        assert p.wait() == 0
        return result
