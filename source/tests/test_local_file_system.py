# pylint: disable=protected-access
import datetime
import itertools
import os
import random
from pathlib import Path
from typing import Dict, Optional
import stat

import pytest

from hashback import protocol
from hashback.local_file_system import AsyncFile, BytesReader, LocalDirectoryExplorer, LocalFileSystemExplorer


@pytest.mark.asyncio
async def test_simple_read_file(tmp_path: Path):
    random_bytes = random.randbytes(200)
    test_file = tmp_path / 'test_file'
    with test_file.open('wb') as file:
        file.write(random_bytes)

    result_bytes = bytes()
    with AsyncFile(test_file, 'r') as file:
        assert file.file_size == len(random_bytes)
        for _ in range(3):
            # This deliberately cuts the content to check the reader buffers correctly.
            result_bytes += await file.read(100)

    assert result_bytes == random_bytes


@pytest.mark.asyncio
async def test_simple_write_file(tmp_path: Path):
    random_bytes = random.randbytes(200)
    test_file = tmp_path / 'test_file'

    with AsyncFile(test_file, 'w') as file:
        await file.write(random_bytes[:100])
        await file.write(random_bytes[100:])
        assert file.tell() == len(random_bytes)

    with test_file.open('rb') as file:
        result_bytes = file.read()

    assert result_bytes == random_bytes


@pytest.mark.asyncio
async def test_bytes_reader_read():
    random_bytes = random.randbytes(200)

    result_bytes = bytes()
    with BytesReader(random_bytes) as file:
        assert file.file_size == len(random_bytes)
        bytes_read = await file.read(25)
        while bytes_read:
            assert len(bytes_read) <= 25
            result_bytes += bytes_read
            bytes_read = await file.read(25)

        assert result_bytes == random_bytes
        assert len(await file.read(25)) == 0


def test_directory_explorer_is_well_named(tmp_path: Path):
    file_explorer = LocalFileSystemExplorer()
    explorer = file_explorer(tmp_path)
    assert str(explorer) == str(tmp_path)


@pytest.mark.asyncio
async def test_basic_directory_iteration(tmp_path: Path):
    file_name = 'some_file'
    dir_name = 'some_directory'

    (tmp_path / dir_name / 'child').mkdir(parents=True)
    with (tmp_path / file_name).open('w') as file:
        file.write("Hello World")

    time_now = datetime.datetime.now(datetime.timezone.utc)
    time_now = time_now.replace(microsecond=round(time_now.microsecond, -3))
    os.utime(tmp_path / 'some_file', (time_now.timestamp(), time_now.timestamp()))

    file_list = {}
    file_explorer = LocalFileSystemExplorer()
    explorer = file_explorer(tmp_path)
    async for name, inode in explorer.iter_children():
        file_list[name] = inode

    # Inode cache should only include files and not directories.
    assert len(file_explorer._all_files) == 1

    assert len(file_list) == 2
    assert file_list[dir_name].type is protocol.FileType.DIRECTORY
    assert file_list[dir_name].size is None
    assert file_list[dir_name].modified_time is None

    assert file_list[file_name] in file_explorer._all_files.values()
    assert file_list[file_name].type is protocol.FileType.REGULAR
    assert file_list[file_name].size == len("Hello World".encode())
    assert (file_list[file_name].modified_time - time_now).microseconds < 100


@pytest.mark.asyncio
async def test_filter_pure_exclude(tmp_path: Path):
    hidden = tmp_path / 'a' / 'b' / 'c' / 'hidden'
    hidden.parent.mkdir(parents=True)
    hidden.touch(exist_ok=False)

    shown = tmp_path / 'shown'
    shown.touch(exist_ok=False)

    file_list = {}
    file_explorer = LocalFileSystemExplorer()
    filters = (
        protocol.Filter(filter=protocol.FilterType.EXCLUDE, path='a'),
    )
    explorer = file_explorer(str(tmp_path), filters)
    async for name, inode in explorer.iter_children():
        file_list[name] = inode

    assert len(file_list) == 1
    assert 'a' not in file_list
    assert file_list['shown'].type is protocol.FileType.REGULAR


@pytest.mark.asyncio
async def test_filter_exclude_exception(tmp_path: Path):
    hidden = tmp_path / 'c' / 'd' / 'e' / 'hidden'
    hidden.parent.mkdir(parents=True)
    hidden.touch(exist_ok=False)

    shown = tmp_path / 'shown'
    shown.touch(exist_ok=False)

    hidden2 = tmp_path / 'c' / 'hidden'

    file_list = {}
    file_explorer = LocalFileSystemExplorer()
    filters = (
        protocol.Filter(filter=protocol.FilterType.EXCLUDE, path='c'),
        protocol.Filter(filter=protocol.FilterType.INCLUDE, path='c/d/e'),
    )
    explorer = file_explorer(tmp_path, filters)
    async for name, inode in explorer.iter_children():
        file_list[name] = inode

    assert len(file_list) == 2
    assert 'c' in file_list
    assert file_list[shown.name].type is protocol.FileType.REGULAR

    # ... And then check the child behaves correctly
    file_list = {}
    explorer = explorer.get_child('c')
    async for name, inode in explorer.iter_children():
        file_list[name] = inode

    assert len(file_list) == 1
    assert hidden2.name not in file_list
    assert 'd' in file_list


@pytest.mark.asyncio
async def test_filter_pattern(tmp_path: Path):
    (tmp_path / 'foo.txt').touch()
    (tmp_path / 'foo.jpg').touch()

    filters = (
        protocol.Filter(filter=protocol.FilterType.PATTERN_EXCLUDE, path='*.txt'),
    )

    file_system_explorer = LocalFileSystemExplorer()
    explorer = file_system_explorer(tmp_path, filters=filters)

    file_list = {}
    async for name, inode in explorer.iter_children():
        file_list[name] = inode

    assert 'foo.txt' not in file_list
    assert 'foo.jpg' in file_list


@pytest.mark.asyncio
async def test_reuse_inode(tmp_path: Path):
    name = 'foo.txt'
    with (tmp_path / name).open('w') as file:
        file.write("Hello")
    (tmp_path / 'a').mkdir()

    (tmp_path / name).link_to(tmp_path /'a' /'bar.txt')

    file_system_explorer = LocalFileSystemExplorer()

    explorer = file_system_explorer(tmp_path)
    async for _, inode in explorer.iter_children():
        if inode.type is protocol.FileType.REGULAR:
            break
    else:
        assert False, 'File not found'

    # Completely new directory explorer from the same parent filesystem explorer looking at the child directory
    explorer = file_system_explorer(tmp_path).get_child('a')
    async for _, inode2 in explorer.iter_children():
        break
    else:
        assert False, "File not found"

    # What should happen here is that the same file object will be returned because it's the same inode
    # pylint: disable=undefined-loop-variable
    assert inode is inode2


@pytest.mark.asyncio
async def test_root_inode(tmp_path: Path):
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    result = await explorer.inode()
    assert result.type is protocol.FileType.DIRECTORY
    assert result != LocalDirectoryExplorer._EXCLUDED_DIR_INODE


@pytest.mark.asyncio
async def test_root_inode_excluded(tmp_path: Path):
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path, (protocol.Filter(filter=protocol.FilterType.EXCLUDE, path="."),))

    result = await explorer.inode()
    assert result == LocalDirectoryExplorer._EXCLUDED_DIR_INODE


@pytest.mark.asyncio
async def test_open_link(tmp_path: Path):
    nowhere = tmp_path / 'nowhere'
    source_path = tmp_path / 'source'

    source_path.symlink_to(nowhere)

    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    with await explorer.open_child(source_path.name) as file:
        result = await file.read(protocol.READ_SIZE)

    assert result == str(nowhere).encode()


@pytest.mark.asyncio
async def test_open_regular(tmp_path: Path):
    source_path = tmp_path / 'source'
    content = random.randbytes(200)
    with source_path.open('wb') as file:
        file.write(content)

    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    with await explorer.open_child(source_path.name) as file:
        result = await file.read(protocol.READ_SIZE)

    assert result == content


@pytest.mark.asyncio
async def test_open_pipe(tmp_path: Path):
    source_path = tmp_path / 'pipe'
    os.mkfifo(source_path)

    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    with await explorer.open_child(source_path.name) as file:
        result = await file.read(protocol.READ_SIZE)

    assert result == bytes()


@pytest.mark.asyncio
async def test_restore_regular(tmp_path):
    content = random.randbytes(200)
    target = tmp_path / 'target'
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    await explorer.restore_child(target.name, protocol.FileType.REGULAR, BytesReader(content), False)

    with target.open('rb') as file:
        result = file.read()
    assert result == content


def _content_for_type(file_type: protocol.FileType) -> Optional[protocol.FileReader]:
    if file_type is protocol.FileType.DIRECTORY:
        return None
    if file_type is protocol.FileType.PIPE:
        return BytesReader(bytes())
    return BytesReader(b'Hello world')


@pytest.mark.asyncio
@pytest.mark.parametrize(('new_type', 'original_type'), itertools.product(
    LocalDirectoryExplorer._INCLUDED_FILE_TYPES, LocalDirectoryExplorer._INCLUDED_FILE_TYPES))
async def test_restore_clobber(tmp_path: Path, new_type: protocol.FileType, original_type: protocol.FileType):
    target_path = tmp_path / 'target'
    fs_explorer = LocalFileSystemExplorer()

    content = _content_for_type(original_type)
    explorer = fs_explorer(tmp_path)
    await explorer.restore_child(target_path.name, original_type, content, clobber_existing=True)

    content = _content_for_type(new_type)
    explorer = fs_explorer(tmp_path)
    if original_type is protocol.FileType.DIRECTORY and new_type is not protocol.FileType.DIRECTORY:
        # We want this to fail.  Clobbering an entire directory tree would be bad!
        with pytest.raises(OSError):
            await explorer.restore_child(target_path.name, new_type, content, clobber_existing=True)
    else:
        await explorer.restore_child(target_path.name, new_type, content, clobber_existing=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(('new_type', 'original_type'), itertools.product(
    LocalDirectoryExplorer._INCLUDED_FILE_TYPES, LocalDirectoryExplorer._INCLUDED_FILE_TYPES))
async def test_restore_no_clobber(tmp_path: Path, new_type: protocol.FileType, original_type: protocol.FileType):
    target_path = tmp_path / 'target'
    fs_explorer = LocalFileSystemExplorer()

    content = _content_for_type(original_type)
    explorer = fs_explorer(tmp_path)
    await explorer.restore_child(target_path.name, original_type, content, clobber_existing=True)

    content = _content_for_type(new_type)
    explorer = fs_explorer(tmp_path)
    if (original_type is protocol.FileType.DIRECTORY and new_type is protocol.FileType.DIRECTORY) or (
        original_type is protocol.FileType.PIPE and new_type is protocol.FileType.PIPE):
        # Replacing a directory with a directory is fine, new and old just merge
        # Replacing a pipe with a pipe is fine, pipes have no content to clobber, the code should do nothing.
        await explorer.restore_child(target_path.name, new_type, content, clobber_existing=False)
    else:
        # We want this to fail.  Clobber=False
        with pytest.raises(OSError):
            await explorer.restore_child(target_path.name, new_type, content, clobber_existing=False)


@pytest.mark.asyncio
@pytest.mark.parametrize('toggles', [
    {'uid': False, 'gid': False}, # Change everything but ownership
    {'uid': False, 'gid': False, 'modified_time': False}, # Change mode
    {'uid': False, 'gid': False, 'mode': False}, # Change modified_time
    {'uid': False, 'gid': True},
    {'uid': True, 'gid': False},
    {'uid': True, 'gid': False},
])
async def test_restore_meta(tmp_path: Path, toggles: Dict[str, bool]):
    if toggles.get('uid', True) or toggles.get('gid', True) and not os.getuid() == 0:
        pytest.skip("Cannot test ownership changes without being root")

    target_path = tmp_path / 'target'
    target_path.touch()

    new_time = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) - datetime.timedelta(days=1)
    new_uid = os.getuid() + 1
    new_gid = os.getgid() + 1
    new_mode = 0o600
    meta = protocol.Inode(type=protocol.FileType.REGULAR, mode=new_mode, modified_time=new_time, size=0,
                          uid=new_uid, gid=new_gid)

    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    await explorer.restore_meta(target_path.name, meta, toggles)

    result_stat = target_path.stat()

    assert (result_stat.st_uid == new_uid) == toggles.get('uid', True)
    assert (result_stat.st_gid == new_gid) == toggles.get('gid', True)
    assert (result_stat.st_mtime == new_time.timestamp()) == toggles.get('modified_time', True)
    assert (stat.S_IMODE(result_stat.st_mode) == new_mode) == toggles.get('mode', True)


@pytest.mark.asyncio
async def test_restore_link(tmp_path):
    link_to = tmp_path / 'link_to'
    link_from = tmp_path / 'link_from'
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    await explorer.restore_child(link_from.name, protocol.FileType.LINK, BytesReader(str(link_to).encode()), False)

    assert not link_to.is_symlink()
    assert link_from.readlink() == link_to


@pytest.mark.asyncio
async def test_restore_pipe(tmp_path):
    target = tmp_path / 'some_fifo'

    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    await explorer.restore_child(target.name, protocol.FileType.PIPE, BytesReader(bytes()), False)


def test_refuse_directory_explorer_for_nonexistent_dir(tmp_path: Path):
    explorer = LocalFileSystemExplorer()
    with pytest.raises(FileNotFoundError):
        explorer(tmp_path / 'not-exists')


def test_value_error_for_file_not_dir(tmp_path: Path):
    dummy_file = tmp_path / 'dummy'
    dummy_file.touch()
    explorer = LocalFileSystemExplorer()
    with pytest.raises(ValueError):
        explorer(dummy_file)


def test_get_path_child(tmp_path: Path):
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    result = explorer.get_path('child_name')
    assert result == str(tmp_path / 'child_name')


def test_get_path_parent(tmp_path: Path):
    fs_explorer = LocalFileSystemExplorer()
    explorer = fs_explorer(tmp_path)

    result = explorer.get_path(None)
    assert result == str(tmp_path)
