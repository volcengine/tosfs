# ByteDance Volcengine EMR, Copyright 2024.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import os
import tempfile
from typing import Any

import pytest

from tosfs.utils import create_temp_dir, random_str


def test_copy(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Create a temporary directory and files
    dir_name = random_str()
    subdir_path = f"{bucket}/{temporary_workspace}/{dir_name}"
    fsspecfs.mkdir(subdir_path)
    file1_path = f"{subdir_path}/file1.txt"
    file2_path = f"{subdir_path}/file2.txt"

    with fsspecfs.open(file1_path, "wb") as f:
        f.write(b"Hello, World!")
    with fsspecfs.open(file2_path, "wb") as f:
        f.write(b"Goodbye, World!")

    # Test Case 1: Copy single file
    copy_dest = f"{bucket}/{temporary_workspace}/copy_file1.txt"
    fsspecfs.copy(file1_path, copy_dest)
    assert fsspecfs.exists(copy_dest), "Failed to copy single file"
    assert (
        fsspecfs.cat_file(copy_dest) == b"Hello, World!"
    ), "Content mismatch in copied file"

    # Test Case 2: Copy directory recursively
    copy_dir_dest = f"{bucket}/{temporary_workspace}/copy_dir"
    fsspecfs.mkdir(copy_dir_dest)
    fsspecfs.copy(subdir_path.rstrip("/") + "/", copy_dir_dest, recursive=True)
    assert fsspecfs.exists(
        f"{copy_dir_dest}/file1.txt"
    ), "Failed to copy directory recursively"
    assert fsspecfs.exists(
        f"{copy_dir_dest}/file2.txt"
    ), "Failed to copy directory recursively"
    assert (
        fsspecfs.cat_file(f"{copy_dir_dest}/file1.txt") == b"Hello, World!"
    ), "Content mismatch in copied directory"
    assert (
        fsspecfs.cat_file(f"{copy_dir_dest}/file2.txt") == b"Goodbye, World!"
    ), "Content mismatch in copied directory"

    # Test Case 3: Copy non-existent file with on_error="ignore"
    non_existent_path = f"{subdir_path}/nonexistent.txt"
    copy_non_existent_dest = f"{bucket}/{temporary_workspace}/copy_nonexistent.txt"
    fsspecfs.copy(non_existent_path, copy_non_existent_dest, on_error="ignore")
    assert not fsspecfs.exists(
        copy_non_existent_dest
    ), "Non-existent file should not be copied"

    # Test Case 4: Copy non-existent file with on_error="raise"
    with pytest.raises(FileNotFoundError):
        fsspecfs.copy(non_existent_path, copy_non_existent_dest, on_error="raise")

    # Test Case 5: Copy file when dest/target exists
    copy_dest = f"{bucket}/{temporary_workspace}/copy_file1.txt"
    assert fsspecfs.exists(copy_dest), "Destination file should exist"
    assert fsspecfs.cat_file(copy_dest) == b"Hello, World!", "Content mismatch"
    fsspecfs.copy(file2_path, copy_dest)
    assert fsspecfs.exists(copy_dest), "Failed to copy and overwrite existing file"
    assert (
        fsspecfs.cat_file(copy_dest) == b"Goodbye, World!"
    ), "Content mismatch in overwritten file"


def test_info(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Setup
    file_path = f"{bucket}/{temporary_workspace}/test_file.txt"
    dir_path = f"{bucket}/{temporary_workspace}/test_dir"
    nested_file_path = f"{dir_path}/nested_file.txt"
    fsspecfs.mkdir(dir_path)
    with fsspecfs.open(file_path, "wb") as f:
        f.write(b"Hello, World!")
    with fsspecfs.open(nested_file_path, "wb") as f:
        f.write(b"Nested Content")

    # Test file info
    file_info = fsspecfs.info(file_path)
    assert file_info["name"] == fsspecfs._strip_protocol(
        file_path
    ), "Incorrect file name"
    expected_file_size = 13
    assert file_info["size"] == expected_file_size, "Incorrect file size"
    assert file_info["type"] == "file", "Incorrect type for file"

    # Test directory info
    dir_info = fsspecfs.info(dir_path)
    assert dir_info["name"] == fsspecfs._strip_protocol(
        dir_path
    ), "Incorrect directory name"
    assert dir_info["type"] == "directory", "Incorrect type for directory"
    # Some FS might not support 'size' for directories, so it's not strictly checked

    # Test non-existent path
    with pytest.raises(FileNotFoundError):
        fsspecfs.info(f"{bucket}/{temporary_workspace}/non_existent")

    # Test protocol stripping
    protocol_included_path = fsspecfs._strip_protocol(file_path)
    protocol_info = fsspecfs.info(protocol_included_path)
    assert protocol_info["name"] == protocol_included_path, "Protocol stripping failed"


def test_write_and_read_bytes(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path_to_write = f"{bucket}/{temporary_workspace}/{file_name}.bin"
    data_to_write = b"Hello, World!"

    with fsspecfs.open(path_to_write, "wb") as f:
        f.write(data_to_write)

    assert fsspecfs.exists(path_to_write), f"Path {path_to_write} does not exist."

    with fsspecfs.open(path_to_write, "rb") as f:
        data_to_read = f.read()

    assert (
        data_to_read == data_to_write
    ), f"Data read from {path_to_write} is not the same as data written."


def test_write_and_read_text(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path_to_write = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data_to_write = "Hello, World!"

    with fsspecfs.open(path_to_write, "w") as f:
        f.write(data_to_write)

    assert fsspecfs.exists(path_to_write), f"Path {path_to_write} does not exist."

    with fsspecfs.open(path_to_write, "r") as f:
        data_to_read = f.read()

    assert (
        data_to_read == data_to_write
    ), f"Data read from {path_to_write} is not the same as data written."


def test_with_size(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"

    expected_file_size = 10

    data = b"a" * (expected_file_size * 1**10)

    with fsspecfs.open(path, "wb") as f:
        f.write(data)

    with fsspecfs.open(path, "rb", size=10) as f:
        assert f.size == expected_file_size
        out = f.read()
        assert len(out) == expected_file_size


def test_simple(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b"a" * (10 * 1**10)

    with fsspecfs.open(path, "wb") as f:
        f.write(data)

    with fsspecfs.open(path, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_write_large(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    mb = 2**20
    payload_size = int(2.5 * 1 * mb)
    payload = b"0" * payload_size

    with fsspecfs.open(path, "wb") as fd:
        fd.write(payload)

    assert fsspecfs.info(path)["size"] == payload_size


def test_write_limit(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    mb = 2**20
    block_size = 1 * mb
    payload_size = 2 * mb
    payload = b"0" * payload_size

    with fsspecfs.open(path, "wb", blocksize=block_size) as fd:
        fd.write(payload)

    assert fsspecfs.info(path)["size"] == payload_size


def test_readline(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"

    lines_to_write = [b"First line\n", b"Second line\n", b"Third line"]

    with fsspecfs.open(path, "wb") as f:
        for line in lines_to_write:
            f.write(line)

    with fsspecfs.open(path, "rb") as f:
        for expected_line in lines_to_write:
            read_line = f.readline()
            assert (
                read_line == expected_line
            ), f"Expected {expected_line}, got {read_line}"

        assert f.readline() == b"", "Expected empty string when reading past the end"


def test_readline_empty(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b""
    with fsspecfs.open(path, "wb") as f:
        f.write(data)
    with fsspecfs.open(path, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b"ab\n" + b"a" * (1 * 2**20) + b"\nab"
    with fsspecfs.open(path, "wb") as f:
        f.write(data)
    with fsspecfs.open(path, "rb") as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (1 * 2**20) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.csv"

    csv_content = b"name,amount,id\nAlice,100,1\nBob,200,2\nCharlie,300,3\n"

    with fsspecfs.open(path, "wb") as f:
        f.write(csv_content)

    with fsspecfs.open(path, "rb") as f:
        # Step 5: Use __next__ to read the first line and assert
        expected = csv_content.split(b"\n")[0] + b"\n"
        result = next(f)
        assert result == expected, f"Expected {expected}, got {result}"

    fsspecfs.rm(path)


def test_iterable(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}"
    data = b"abc\n123"
    with fsspecfs.open(path, "wb") as f:
        f.write(data)
    with fsspecfs.open(path) as f, io.BytesIO(data) as g:
        for froms3, fromio in zip(f, g):
            assert froms3 == fromio
        f.seek(0)
        assert f.readline() == b"abc\n"
        assert f.readline() == b"123"
        f.seek(1)
        assert f.readline() == b"bc\n"

    with fsspecfs.open(path) as f:
        out = list(f)
    with fsspecfs.open(path) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_write_read_without_protocol(
    fsspecfs: Any, bucket: str, temporary_workspace: str
):
    file_name = random_str()
    path_to_write = f"{bucket}/{temporary_workspace}/{file_name}.bin"
    path_without_protocol = fsspecfs._strip_protocol(path_to_write)
    data_to_write = b"Hello, World!"

    with fsspecfs.open(path_without_protocol, "wb") as f:
        f.write(data_to_write)

    assert fsspecfs.exists(
        path_without_protocol
    ), f"Path {path_without_protocol} does not exist."

    with fsspecfs.open(f"/{path_without_protocol}", "rb") as f:
        data_to_read = f.read()

    assert (
        data_to_read == data_to_write
    ), f"Data read from {path_to_write} is not the same as data written."


def test_walk(fsspecfs: Any, bucket: str, temporary_workspace: str):
    nested_dir_1 = f"{bucket}/{temporary_workspace}/nested_dir_1"
    nested_dir_2 = f"{nested_dir_1}/nested_dir_2"
    file_1 = f"{bucket}/{temporary_workspace}/file_1.txt"
    file_2 = f"{nested_dir_1}/file_2.txt"
    file_3 = f"{nested_dir_2}/file_3.txt"

    fsspecfs.mkdir(nested_dir_1)
    fsspecfs.mkdir(nested_dir_2)
    with fsspecfs.open(file_1, "wb") as f:
        f.write(b"File 1 content")
    with fsspecfs.open(file_2, "wb") as f:
        f.write(b"File 2 content")
    with fsspecfs.open(file_3, "wb") as f:
        f.write(b"File 3 content")

    # Test walk with maxdepth=None and topdown=True
    result = list(
        fsspecfs.walk(f"{bucket}/{temporary_workspace}", maxdepth=None, topdown=True)
    )
    expected = [
        (
            fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}"),
            ["nested_dir_1"],
            ["file_1.txt"],
        ),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
        (fsspecfs._strip_protocol(nested_dir_2), [], ["file_3.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=1 and topdown=True
    result = list(
        fsspecfs.walk(f"{bucket}/{temporary_workspace}", maxdepth=1, topdown=True)
    )
    expected = [
        (
            fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}"),
            ["nested_dir_1"],
            ["file_1.txt"],
        ),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=2 and topdown=True
    result = list(
        fsspecfs.walk(f"{bucket}/{temporary_workspace}", maxdepth=2, topdown=True)
    )
    expected = [
        (
            fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}"),
            ["nested_dir_1"],
            ["file_1.txt"],
        ),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=None and topdown=False
    result = list(
        fsspecfs.walk(f"{bucket}/{temporary_workspace}", maxdepth=None, topdown=False)
    )
    expected = [
        (fsspecfs._strip_protocol(nested_dir_2), [], ["file_3.txt"]),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
        (
            fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}"),
            ["nested_dir_1"],
            ["file_1.txt"],
        ),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with detail=True
    result = list(
        fsspecfs.walk(
            f"{bucket}/{temporary_workspace}", maxdepth=None, topdown=True, detail=True
        )
    )
    expected_dir_num = 3
    assert (
        len(result) == expected_dir_num
    ), f"Expected {expected_dir_num} directories, got {len(result)}"
    for _, dirs, files in result:
        assert isinstance(dirs, dict), f"Expected dirs to be dict, got {type(dirs)}"
        assert isinstance(files, dict), f"Expected files to be dict, got {type(files)}"


def test_find(fsspecfs: Any, bucket: str, temporary_workspace: str):
    def remove_last_modification_time_ms(data):
        if isinstance(data, dict):
            for key in data:
                if "LastModified" in data[key]:
                    del data[key]["LastModified"]
        return data

    file1_path = f"{bucket}/{temporary_workspace}/file1"
    file2_path = f"{bucket}/{temporary_workspace}/file2"
    dir1_path = f"{bucket}/{temporary_workspace}/dir1"
    dir2_path = f"{bucket}/{temporary_workspace}/dir2"
    file3_path = f"{dir1_path}/file3"
    file4_path = f"{dir2_path}/file4"

    fsspecfs.mkdir(dir1_path)
    fsspecfs.mkdir(dir2_path)

    fsspecfs.touch(file1_path)
    fsspecfs.touch(file2_path)
    fsspecfs.touch(file3_path)
    fsspecfs.touch(file4_path)

    # Test finding all files
    result = fsspecfs.find(f"{bucket}/{temporary_workspace}")
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding files with maxdepth=1
    result = fsspecfs.find(f"{bucket}/{temporary_workspace}", maxdepth=1)
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding files and directories
    result = fsspecfs.find(f"{bucket}/{temporary_workspace}", withdirs=True)
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert sorted(result) == sorted(expected), f"Expected {expected}, got {result}"

    # Test finding files with detail=True
    result = fsspecfs.find(f"{bucket}/{temporary_workspace}", detail=True)
    result = remove_last_modification_time_ms(result)
    expected = {
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"): {
            "Key": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir1/file3"
            ),
            "name": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir1/file3"
            ),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"): {
            "Key": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir2/file4"
            ),
            "name": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir2/file4"
            ),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"): {
            "Key": fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
            "name": fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"): {
            "Key": fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
            "name": fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
            "type": "file",
            "size": 0,
        },
    }
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding a single file
    result = fsspecfs.find(f"{bucket}/{temporary_workspace}/file1")
    expected = [fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1")]
    assert result == expected, f"Expected {expected}, got {result}"


def test_du(fsspecfs: Any, bucket: str, temporary_workspace: str):
    dir_path = f"{bucket}/{temporary_workspace}/test_dir"
    nested_dir_path = f"{dir_path}/nested_dir"
    file_path = f"{dir_path}/test_file.txt"
    nested_file_path = f"{nested_dir_path}/nested_file.txt"

    fsspecfs.mkdir(dir_path)
    fsspecfs.mkdir(nested_dir_path)

    with fsspecfs.open(file_path, "wb") as f:
        f.write(b"Hello, World!")
    with fsspecfs.open(nested_file_path, "wb") as f:
        f.write(b"Nested Content")

    # Test total size
    total_size = fsspecfs.du(f"{bucket}/{temporary_workspace}", total=True)
    assert (
        total_size == 13 + 14
    ), f"Expected total size to be {13 + 14}, got {total_size}"

    # Test individual sizes
    sizes = fsspecfs.du(f"{bucket}/{temporary_workspace}", total=False)
    expected_sizes = {
        fsspecfs._strip_protocol(file_path): 13,
        fsspecfs._strip_protocol(nested_file_path): 14,
    }
    assert sizes == expected_sizes, f"Expected sizes {expected_sizes}, got {sizes}"

    with pytest.raises(ValueError, match="maxdepth must be at least 1"):
        fsspecfs.du(f"{bucket}/{temporary_workspace}", total=False, maxdepth=0)

    # Test maxdepth
    sizes_maxdepth_1 = fsspecfs.du(
        f"{bucket}/{temporary_workspace}", total=False, maxdepth=2
    )
    expected_sizes_maxdepth_1 = {fsspecfs._strip_protocol(file_path): 13}
    assert (
        sizes_maxdepth_1 == expected_sizes_maxdepth_1
    ), f"Expected sizes {expected_sizes_maxdepth_1}, got {sizes_maxdepth_1}"


def test_isdir(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Setup
    dir_name = random_str()
    file_name = random_str()
    dir_path = f"{bucket}/{temporary_workspace}/{dir_name}"
    file_path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    fsspecfs.mkdir(dir_path)
    with fsspecfs.open(file_path, "wb") as f:
        f.write(b"Hello, World!")

    # Test directory
    assert fsspecfs.isdir(dir_path), f"Expected {dir_path} to be a directory"

    # Test file
    assert not fsspecfs.isdir(file_path), f"Expected {file_path} to be a file"

    # Test non-existent path
    non_existent_path = f"{bucket}/{temporary_workspace}/non_existent"
    assert not fsspecfs.isdir(
        non_existent_path
    ), f"Expected {non_existent_path} to not exist"


def test_isfile(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Setup
    dir_name = random_str()
    file_name = random_str()
    dir_path = f"{bucket}/{temporary_workspace}/{dir_name}"
    file_path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    fsspecfs.mkdir(dir_path)
    with fsspecfs.open(file_path, "wb") as f:
        f.write(b"Hello, World!")

    # Test file
    assert fsspecfs.isfile(file_path), f"Expected {file_path} to be a file"

    # Test directory
    assert not fsspecfs.isfile(dir_path), f"Expected {dir_path} to be a directory"

    # Test non-existent path
    non_existent_path = f"{bucket}/{temporary_workspace}/non_existent"
    assert not fsspecfs.isfile(
        non_existent_path
    ), f"Expected {non_existent_path} to not exist"


def test_rm(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_names = [random_str() for _ in range(5)]
    paths_to_remove = [
        f"{bucket}/{temporary_workspace}/{file_name}.txt" for file_name in file_names
    ]

    for path in paths_to_remove:
        with fsspecfs.open(path, "wb") as f:
            f.write(b"Temporary content")

    fsspecfs.rm(paths_to_remove, recursive=True)

    for path in paths_to_remove:
        assert not fsspecfs.exists(path), f"Path {path} still exists after removal"

    # Remove scheme from paths and test removal again
    paths_without_scheme = [fsspecfs._strip_protocol(path) for path in paths_to_remove]

    for path in paths_without_scheme:
        with fsspecfs.open(path, "wb") as f:
            f.write(b"Temporary content")

    fsspecfs.rm(paths_without_scheme, recursive=True)

    for path in paths_without_scheme:
        assert not fsspecfs.exists(path), f"Path {path} still exists after removal"


def test_cat_file(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path_to_write = f"{bucket}/{temporary_workspace}/{file_name}.bin"
    content = b"Hello, World! This is a test file."
    with fsspecfs.open(path_to_write, "wb") as f:
        f.write(content)

    assert fsspecfs.cat_file(path_to_write) == content, "Failed to read the entire file"

    start = 7
    assert (
        fsspecfs.cat_file(path_to_write, start=start) == content[start:]
    ), "Failed to read from a specific start to the end"

    start, end = 7, 22
    assert (
        fsspecfs.cat_file(path_to_write, start=start, end=end) == content[start:end]
    ), "Failed to read a specific range"

    assert (
        fsspecfs.cat_file(path_to_write, start=-22, end=-7) == content[-22:-7]
    ), "Failed to read with negative indices"


def test_cat(fsspecfs: Any, bucket: str, temporary_workspace: str):
    dir_name = random_str()
    subdir_path = f"{bucket}/{temporary_workspace}/{dir_name}"
    fsspecfs.mkdir(subdir_path)
    file1_path = f"{subdir_path}/file1.txt"
    file2_path = f"{subdir_path}/file2.txt"

    with fsspecfs.open(file1_path, "wb") as f:
        f.write(b"Hello, World!")
    with fsspecfs.open(file2_path, "wb") as f:
        f.write(b"Goodbye, World!")

    # Test Case 1: Single file, recursive=False
    result = fsspecfs.cat(file1_path, recursive=False)
    assert result == b"Hello, World!", "Single file content mismatch"

    # Test Case 2: Directory, recursive=True
    result = fsspecfs.cat(f"{subdir_path}/*", recursive=True)
    expected = {
        fsspecfs._strip_protocol(file1_path): b"Hello, World!",
        fsspecfs._strip_protocol(file2_path): b"Goodbye, World!",
    }
    assert result == expected, "Directory recursive content mismatch"

    # Test Case 3: Error handling with on_error="omit"
    non_existent_path = f"{subdir_path}/nonexistent.txt"
    result = fsspecfs.cat(
        [file1_path, non_existent_path], recursive=True, on_error="omit"
    )
    assert result == {
        fsspecfs._strip_protocol(file1_path): b"Hello, World!"
    }, "Error handling with omit failed"

    result = fsspecfs.cat(f"{subdir_path}/*", recursive=True, on_error="omit")
    assert result == {
        fsspecfs._strip_protocol(file1_path): b"Hello, World!",
        fsspecfs._strip_protocol(file2_path): b"Goodbye, World!",
    }, "Error handling with omit failed"

    # Test Case 4: Error handling with on_error="return"
    result = fsspecfs.cat([file1_path, non_existent_path], on_error="return")
    file1_in_result = fsspecfs._strip_protocol(file1_path) in result
    assert file1_in_result, "file1_path is not in the result"

    non_existent_in_result = isinstance(
        result[fsspecfs._strip_protocol(non_existent_path)], Exception
    )
    assert non_existent_in_result, "non_existent_path is not an Exception in the result"

    fsspecfs.rm(subdir_path, recursive=True)


def test_mv(fsspecfs: Any, bucket: str, temporary_workspace: str):
    source_folder = f"{bucket}/{temporary_workspace}/source_folder"
    target_folder = f"{bucket}/{temporary_workspace}/target_folder"
    test_file_name = "test_file.txt"
    renamed_file_name = "renamed_file.txt"
    test_file_content = b"Hello, ProtonFS!"
    target_file_content = b"Old content"

    fsspecfs.mkdir(source_folder)
    fsspecfs.mkdir(target_folder)
    source_file_path = f"{source_folder}/{test_file_name}"
    target_file_path = f"{target_folder}/{test_file_name}"
    with fsspecfs.open(source_file_path, "wb") as f:
        f.write(test_file_content)

    with fsspecfs.open(target_file_path, "wb") as f:
        f.write(target_file_content)

    fsspecfs.mv(source_file_path, target_file_path)

    assert not fsspecfs.exists(
        source_file_path
    ), "Source file should not exist after move"
    assert fsspecfs.exists(target_file_path), "Target file should exist after move"

    with fsspecfs.open(target_file_path, "rb") as f:
        content = f.read()
        assert (
            content == test_file_content
        ), "Target file content should be overwritten by source file content"

    renamed_file_path = f"{target_folder}/{renamed_file_name}"
    fsspecfs.mv(target_file_path, renamed_file_path)

    assert not fsspecfs.exists(
        target_file_path
    ), "Original file should not exist after rename"
    assert fsspecfs.exists(renamed_file_path), "Renamed file should exist"

    # test mv source dir into target dir recursively
    fsspecfs.rm(source_folder, recursive=True)
    fsspecfs.rm(target_folder, recursive=True)
    fsspecfs.mkdir(source_folder)
    fsspecfs.mkdir(target_folder)
    assert fsspecfs.exists(source_folder)
    assert fsspecfs.exists(target_folder)
    source_file = f"{random_str()}.txt"
    with fsspecfs.open(f"{source_folder}/{source_file}", "wb") as f:
        f.write(test_file_content)
    assert fsspecfs.exists(f"{source_folder}/{source_file}")
    fsspecfs.mv(source_folder, target_folder, recursive=True)
    assert fsspecfs.exists(f"{target_folder}/source_folder")
    assert fsspecfs.exists(f"{target_folder}/source_folder/{source_file}")
    assert not fsspecfs.exists(f"{source_folder}")
    assert not fsspecfs.exists(f"{source_folder}/{source_file}")


def test_get(fsspecfs: Any, bucket: str, temporary_workspace: str):
    remote_file_path = f"{bucket}/{temporary_workspace}/test_get_file.txt"
    local_temp_dir = create_temp_dir()
    local_file_path = f"{local_temp_dir}/test_get_file.txt"
    test_data = b"Data for testing ProtonFileSystem#get method."

    if os.path.exists(local_file_path):
        os.remove(local_file_path)

    with fsspecfs.open(remote_file_path, "wb") as f:
        f.write(test_data)

    fsspecfs.get(remote_file_path, local_file_path)

    assert os.path.exists(local_file_path), "The file was not copied to the local path."
    with open(local_file_path, "rb") as f:
        local_data = f.read()
        assert (
            local_data == test_data
        ), "The data in the local file does not match the expected data."

    os.remove(local_file_path)


def test_put(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = f"{random_str()}.txt"
    with tempfile.TemporaryDirectory() as local_temp_dir:
        local_file_path = f"{local_temp_dir}/{file_name}"
        test_data = b"Data for testing ProtonFileSystem put method."

        with open(local_file_path, "wb") as f:
            f.write(test_data)

        remote_file_path = f"{bucket}/{temporary_workspace}/{file_name}"
        fsspecfs.put(local_file_path, remote_file_path)

        assert fsspecfs.exists(
            remote_file_path
        ), "The file should exist in the remote location after upload."

        downloaded_file_path = f"{local_temp_dir}/downloaded_{file_name}"
        fsspecfs.get(remote_file_path, downloaded_file_path)
        with open(downloaded_file_path, "rb") as f:
            downloaded_data = f.read()
            assert (
                downloaded_data == test_data
            ), "The downloaded file's contents should match the original test data."

        # test lpath and rpath are both dirs
        remote_temp_dir = f"{bucket}/{temporary_workspace}/{random_str()}"

        fsspecfs.mkdir(remote_temp_dir)
        fsspecfs.put(local_temp_dir, remote_temp_dir, recursive=True)
        assert fsspecfs.exists(remote_temp_dir), "The remote directory should exist."
        remote_file_path = os.path.join(
            remote_temp_dir, os.path.basename(local_temp_dir), file_name
        )
        assert fsspecfs.exists(
            remote_file_path
        ), "The remote directory should contain the local directory."

        with fsspecfs.open(remote_file_path, "rb") as f:
            downloaded_data = f.read()
            assert (
                downloaded_data == test_data
            ), "The downloaded file's contents should match the original test data."


def test_put_tree(fsspecfs, bucket, temporary_workspace):
    with tempfile.TemporaryDirectory() as local_temp_dir:
        dir_a = f"{local_temp_dir}/a"
        dir_b = f"{local_temp_dir}/b"
        dir_a_a = f"{dir_a}/a"
        dir_a_a_a = f"{dir_a_a}/a"

        # Create tmp.txt in each directory
        for directory in [dir_a, dir_b, dir_a_a, dir_a_a_a]:
            os.mkdir(directory)
            tmp_file_path = os.path.join(directory, "tmp.txt")
            with open(tmp_file_path, "w") as tmp_file:
                tmp_file.write("Temporary file content")

        remote_temp_dir = f"{bucket}/{temporary_workspace}/{random_str()}"
        fsspecfs.mkdir(remote_temp_dir)
        fsspecfs.put(local_temp_dir, remote_temp_dir, recursive=True)

        # Verify the directory tree on the remote filesystem
        expected_structure = [
            (
                fsspecfs._strip_protocol(f"{remote_temp_dir}"),
                [os.path.basename(local_temp_dir)],
                [],
            ),
            (
                fsspecfs._strip_protocol(
                    f"{remote_temp_dir}/{os.path.basename(local_temp_dir)}"
                ),
                ["a", "b"],
                [],
            ),
            (
                fsspecfs._strip_protocol(
                    f"{remote_temp_dir}/{os.path.basename(local_temp_dir)}/a"
                ),
                ["a"],
                ["tmp.txt"],
            ),
            (
                fsspecfs._strip_protocol(
                    f"{remote_temp_dir}/{os.path.basename(local_temp_dir)}/a/a"
                ),
                ["a"],
                ["tmp.txt"],
            ),
            (
                fsspecfs._strip_protocol(
                    f"{remote_temp_dir}/{os.path.basename(local_temp_dir)}/a/a/a"
                ),
                [],
                ["tmp.txt"],
            ),
            (
                fsspecfs._strip_protocol(
                    f"{remote_temp_dir}/{os.path.basename(local_temp_dir)}/b"
                ),
                [],
                ["tmp.txt"],
            ),
        ]

        actual = list(fsspecfs.walk(remote_temp_dir))
        assert actual == expected_structure


def test_fs_read_block(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b"ab\n" + b"a" * (1 * 2**20) + b"\nab"
    with fsspecfs.open(path, "wb") as f:
        f.write(data)

    assert fsspecfs.read_block(path, 0, 3) == b"ab\n"

    non_exist_path = f"{bucket}/{temporary_workspace}/non_exist.txt"
    with pytest.raises(FileNotFoundError):
        fsspecfs.read_block(non_exist_path, 0, 3)


def test_tail(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b"ab\n" + b"a" * (1 * 2**20) + b"\nab"
    with fsspecfs.open(path, "wb") as f:
        f.write(data)

    assert fsspecfs.tail(path, 3) == b"\nab"

    non_exist_path = f"{bucket}/{temporary_workspace}/non_exist.txt"
    with pytest.raises(FileNotFoundError):
        fsspecfs.tail(non_exist_path, 3)


def test_size(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = random_str()
    path = f"{bucket}/{temporary_workspace}/{file_name}"
    content = "hello world"
    with fsspecfs.open(path, "w") as f:
        f.write(content)

    assert fsspecfs.size(path) == len(content)
    assert fsspecfs.size(f"{bucket}/{temporary_workspace}") == 0
