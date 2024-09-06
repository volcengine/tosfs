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
import random
import string
from typing import Any

import pytest


def test_ls(fsspecfs: Any, bucket: str, temporary_workspace: str):
    path_to_find = f"{bucket}/{temporary_workspace}"
    path_exists = fsspecfs.exists(path_to_find)

    assert path_exists


def test_copy(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Create a temporary directory and files
    dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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

    # Clean up
    fsspecfs.rm(subdir_path, recursive=True)
    fsspecfs.rm(copy_dest)
    fsspecfs.rm(copy_dir_dest, recursive=True)


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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b"a" * (10 * 1**10)

    with fsspecfs.open(path, "wb") as f:
        f.write(data)

    with fsspecfs.open(path, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_write_large(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    mb = 2**20
    payload_size = int(2.5 * 1 * mb)
    payload = b"0" * payload_size

    with fsspecfs.open(path, "wb") as fd:
        fd.write(payload)

    assert fsspecfs.info(path)["size"] == payload_size


def test_write_limit(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    mb = 2**20
    block_size = 1 * mb
    payload_size = 2 * mb
    payload = b"0" * payload_size

    with fsspecfs.open(path, "wb", blocksize=block_size) as fd:
        fd.write(payload)

    assert fsspecfs.info(path)["size"] == payload_size


def test_readline(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    path = f"{bucket}/{temporary_workspace}/{file_name}.txt"
    data = b""
    with fsspecfs.open(path, "wb") as f:
        f.write(data)
    with fsspecfs.open(path, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(fsspecfs: Any, bucket: str, temporary_workspace: str):
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    sub_dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    temp_folder = f"{bucket}/{temporary_workspace}/{sub_dir_name}"
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
    result = list(fsspecfs.walk(temp_folder, maxdepth=None, topdown=True))
    expected = [
        (fsspecfs._strip_protocol(temp_folder), ["nested_dir_1"], ["file_1.txt"]),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
        (fsspecfs._strip_protocol(nested_dir_2), [], ["file_3.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=1 and topdown=True
    result = list(fsspecfs.walk(temp_folder, maxdepth=1, topdown=True))
    expected = [
        (fsspecfs._strip_protocol(temp_folder), ["nested_dir_1"], ["file_1.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=2 and topdown=True
    result = list(fsspecfs.walk(temp_folder, maxdepth=2, topdown=True))
    expected = [
        (fsspecfs._strip_protocol(temp_folder), ["nested_dir_1"], ["file_1.txt"]),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with maxdepth=None and topdown=False
    result = list(fsspecfs.walk(temp_folder, maxdepth=None, topdown=False))
    expected = [
        (fsspecfs._strip_protocol(nested_dir_2), [], ["file_3.txt"]),
        (fsspecfs._strip_protocol(nested_dir_1), ["nested_dir_2"], ["file_2.txt"]),
        (fsspecfs._strip_protocol(temp_folder), ["nested_dir_1"], ["file_1.txt"]),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test walk with detail=True
    result = list(fsspecfs.walk(temp_folder, maxdepth=None, topdown=True, detail=True))
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
                if "last_modification_time_ms" in data[key]:
                    del data[key]["last_modification_time_ms"]
        return data

    sub_dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    temp_folder = f"{bucket}/{temporary_workspace}/{sub_dir_name}"
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
    result = fsspecfs.find(temp_folder)
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding files with maxdepth=1
    result = fsspecfs.find(temp_folder, maxdepth=1)
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding files and directories
    result = fsspecfs.find(temp_folder, withdirs=True)
    expected = [
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"),
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Test finding files with detail=True
    result = fsspecfs.find(temp_folder, detail=True)
    result = remove_last_modification_time_ms(result)
    expected = {
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir1/file3"): {
            "name": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir1/file3"
            ),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/dir2/file4"): {
            "name": fsspecfs._strip_protocol(
                f"{bucket}/{temporary_workspace}/dir2/file4"
            ),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"): {
            "name": fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file1"),
            "type": "file",
            "size": 0,
        },
        fsspecfs._strip_protocol(f"{bucket}/{temporary_workspace}/file2"): {
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
    sub_dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    temp_folder = f"{bucket}/{temporary_workspace}/{sub_dir_name}"
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
    total_size = fsspecfs.du(temp_folder, total=True)
    assert (
        total_size == 13 + 14
    ), f"Expected total size to be {13 + 14}, got {total_size}"

    # Test individual sizes
    sizes = fsspecfs.du(temp_folder, total=False)
    expected_sizes = {
        fsspecfs._strip_protocol(file_path): 13,
        fsspecfs._strip_protocol(nested_file_path): 14,
    }
    assert sizes == expected_sizes, f"Expected sizes {expected_sizes}, got {sizes}"

    # Test maxdepth
    sizes_maxdepth_1 = fsspecfs.du(temp_folder, total=False, maxdepth=2)
    expected_sizes_maxdepth_1 = {fsspecfs._strip_protocol(file_path): 13}
    assert (
        sizes_maxdepth_1 == expected_sizes_maxdepth_1
    ), f"Expected sizes {expected_sizes_maxdepth_1}, got {sizes_maxdepth_1}"

    # Test withdirs=True
    sizes_withdirs = fsspecfs.du(temp_folder, total=False, withdirs=True)
    expected_sizes_withdirs = {
        fsspecfs._strip_protocol(temp_folder): 27,
        fsspecfs._strip_protocol(dir_path): 27,
        fsspecfs._strip_protocol(file_path): 13,
        fsspecfs._strip_protocol(nested_dir_path): 14,
        fsspecfs._strip_protocol(nested_file_path): 14,
    }
    assert (
        sizes_withdirs == expected_sizes_withdirs
    ), f"Expected sizes {expected_sizes_withdirs}, got {sizes_withdirs}"

    total_sizes_withdirs = fsspecfs.du(temp_folder, total=True, withdirs=True)
    expected_total_sizes_withdirs = 95
    assert (
        total_sizes_withdirs == expected_total_sizes_withdirs
    ), f"Expected sizes {expected_total_sizes_withdirs}, got {total_sizes_withdirs}"


def test_isdir(fsspecfs: Any, bucket: str, temporary_workspace: str):
    # Setup
    dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    file_names = [
        "".join(random.choices(string.ascii_letters + string.digits, k=10))
        for _ in range(5)
    ]
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
    file_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    dir_name = "".join(random.choices(string.ascii_letters + string.digits, k=10))
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
    result = fsspecfs.cat([file1_path, non_existent_path], on_error="omit")
    assert result == {
        fsspecfs._strip_protocol(file1_path): b"Hello, World!"
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
