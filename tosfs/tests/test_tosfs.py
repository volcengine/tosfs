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
import os.path
import tempfile

import pytest
from tos.exceptions import TosServerError

from tosfs.core import TosFileSystem
from tosfs.exceptions import TosfsError
from tosfs.utils import create_temp_dir, random_str


def test_ls_bucket(tosfs: TosFileSystem, bucket: str) -> None:
    assert bucket in tosfs.ls("", detail=False)
    detailed_list = tosfs.ls("", detail=True)
    assert detailed_list
    for item in detailed_list:
        assert "type" in item
        assert item["type"] == "directory"
        assert item["StorageClass"] == "BUCKET"

    with pytest.raises(TosServerError):
        tosfs.ls("nonexistent", detail=False)


def test_ls_dir(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    assert f"{bucket}/{temporary_workspace}" in tosfs.ls(bucket, detail=False)
    detailed_list = tosfs.ls(bucket, detail=True)
    assert detailed_list
    for item in detailed_list:
        if item["name"] == f"{bucket}/{temporary_workspace}":
            assert item["type"] == "directory"
            break
    else:
        raise AssertionError(
            f"Directory {temporary_workspace} not found in {detailed_list}"
        )

    assert tosfs.ls(f"{bucket}/{temporary_workspace}", detail=False) == []
    assert tosfs.ls(f"{bucket}/{temporary_workspace}/nonexistent", detail=False) == []


def test_inner_rm(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    file_name = random_str()
    tosfs.tos_client.put_object(bucket=bucket, key=f"{temporary_workspace}/{file_name}")
    assert f"{bucket}/{temporary_workspace}/{file_name}" in tosfs.ls(
        f"{bucket}/{temporary_workspace}", detail=False
    )

    tosfs._rm(f"{bucket}/{temporary_workspace}/{file_name}")

    assert tosfs.ls(f"{bucket}/{temporary_workspace}", detail=False) == []

    tosfs._rm(f"{bucket}/{temporary_workspace}/{file_name}")


def test_info(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    assert tosfs.info("") == {"name": "", "size": 0, "type": "directory"}
    assert tosfs.info("/") == {"name": "/", "size": 0, "type": "directory"}
    assert tosfs.info(bucket) == {
        "Key": "proton-ci",
        "Size": 0,
        "StorageClass": "BUCKET",
        "name": "proton-ci",
        "size": 0,
        "type": "directory",
    }
    assert tosfs.info(f"{bucket}/{temporary_workspace}") == {
        "name": f"{bucket}/{temporary_workspace}",
        "type": "directory",
        "size": 0,
        "StorageClass": "DIRECTORY",
    }

    with pytest.raises(FileNotFoundError):
        tosfs.info(f"{bucket}/nonexistent")


def test_rmdir(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    with pytest.raises(TosfsError):
        tosfs.rmdir(bucket)

    file_name = random_str()
    tosfs.tos_client.put_object(bucket=bucket, key=f"{temporary_workspace}/{file_name}")
    assert f"{bucket}/{temporary_workspace}/{file_name}" in tosfs.ls(
        f"{bucket}/{temporary_workspace}", detail=False
    )

    with pytest.raises(TosfsError):
        tosfs.rmdir(f"{bucket}/{temporary_workspace}")

    with pytest.raises(NotADirectoryError):
        tosfs.rmdir(f"{bucket}/{temporary_workspace}/{file_name}")

    tosfs._rm(f"{bucket}/{temporary_workspace}/{file_name}")
    assert tosfs.ls(f"{bucket}/{temporary_workspace}", detail=False) == []

    tosfs.rmdir(f"{bucket}/{temporary_workspace}")
    assert f"{bucket}/{temporary_workspace}" not in tosfs.ls(
        bucket, detail=False, refresh=True
    )


def test_touch(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    file_name = random_str()
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")
    tosfs.touch(f"{bucket}/{temporary_workspace}/{file_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")
    assert tosfs.info(f"{bucket}/{temporary_workspace}/{file_name}")["size"] == 0

    with pytest.raises(FileExistsError):
        tosfs.touch(f"{bucket}/{temporary_workspace}/{file_name}", truncate=False)

    tosfs.rm_file(f"{bucket}/{temporary_workspace}/{file_name}")
    tosfs.touch(f"{bucket}/{temporary_workspace}/{file_name}", truncate=False)
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")

    tosfs.rm_file(f"{bucket}/{temporary_workspace}/{file_name}")
    tosfs.tos_client.put_object(
        bucket=bucket, key=f"{temporary_workspace}/{file_name}", content="hello world"
    )
    assert tosfs.info(f"{bucket}/{temporary_workspace}/{file_name}")["size"] > 0
    tosfs.touch(f"{bucket}/{temporary_workspace}/{file_name}", truncate=True)
    assert tosfs.info(f"{bucket}/{temporary_workspace}/{file_name}")["size"] == 0
    tosfs.rm_file(f"{bucket}/{temporary_workspace}/{file_name}")


def test_isdir(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    assert not tosfs.isdir("")
    assert not tosfs.isdir("/")
    assert not tosfs.isdir(bucket)
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/")
    assert not tosfs.isdir(f"{bucket}/{temporary_workspace}/nonexistent")
    assert not tosfs.isdir(f"{bucket}/{temporary_workspace}/nonexistent/")

    file_name = random_str()
    tosfs.tos_client.put_object(bucket=bucket, key=f"{temporary_workspace}/{file_name}")
    assert not tosfs.isdir(f"{bucket}/{temporary_workspace}/{file_name}")
    assert not tosfs.isdir(f"{bucket}/{temporary_workspace}/{file_name}/")

    tosfs._rm(f"{bucket}/{temporary_workspace}/{file_name}")


def test_isfile(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    file_name = random_str()
    tosfs.tos_client.put_object(bucket=bucket, key=f"{temporary_workspace}/{file_name}")
    assert tosfs.isfile(f"{bucket}/{temporary_workspace}/{file_name}")
    assert not tosfs.isfile(f"{bucket}/{temporary_workspace}/{file_name}/")
    assert not tosfs.isfile(f"{bucket}/{temporary_workspace}/nonexistfile")
    assert not tosfs.isfile(f"{bucket}/{temporary_workspace}")
    assert not tosfs.isfile(f"{bucket}/{temporary_workspace}/")

    tosfs._rm(f"{bucket}/{temporary_workspace}/{file_name}")


def test_exists_bucket(
    tosfs: TosFileSystem, bucket: str, temporary_workspace: str
) -> None:
    assert tosfs.exists("")
    assert tosfs.exists("/")
    assert tosfs.exists(bucket)
    assert not tosfs.exists("nonexistent")


def test_exists_object(
    tosfs: TosFileSystem, bucket: str, temporary_workspace: str
) -> None:
    file_name = random_str()
    tosfs.tos_client.put_object(bucket=bucket, key=f"{temporary_workspace}/{file_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/nonexistent")
    assert not tosfs.exists(f"{bucket}/nonexistent")
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/nonexistent")
    tosfs.rm_file(f"{bucket}/{temporary_workspace}/{file_name}")
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")


def test_mkdir(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    dir_name = random_str()

    with pytest.raises(TosfsError):
        tosfs.mkdir(f"{bucket}")

    with pytest.raises(TosfsError):
        tosfs.mkdir(f"{bucket}/")

    with pytest.raises(TosfsError):
        tosfs.mkdir("/")

    tosfs.mkdir(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/{dir_name}")

    tosfs.rmdir(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")

    tosfs.mkdir(f"{bucket}/{temporary_workspace}/{dir_name}", create_parents=False)
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")

    with pytest.raises(FileNotFoundError):
        tosfs.mkdir(
            f"{bucket}/{temporary_workspace}/notexist/{dir_name}", create_parents=False
        )

    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/notexist")
    tosfs.mkdir(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/notexist/")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/notexist/")

    tosfs.rmdir(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}/notexist")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}/{dir_name}")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}")


def test_makedirs(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    dir_name = random_str()

    with pytest.raises(FileExistsError):
        tosfs.makedirs(f"{bucket}/{temporary_workspace}", exist_ok=False)

    tosfs.makedirs(f"{bucket}/{temporary_workspace}", exist_ok=True)

    tosfs.makedirs(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/{dir_name}")

    tosfs.rmdir(f"{bucket}/{temporary_workspace}/{dir_name}")
    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")

    tosfs.makedirs(f"{bucket}/{temporary_workspace}/{dir_name}", exist_ok=True)
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")

    tosfs.makedirs(f"{bucket}/{temporary_workspace}/{dir_name}", exist_ok=True)
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{dir_name}")

    assert not tosfs.exists(f"{bucket}/{temporary_workspace}/notexist")
    tosfs.makedirs(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/notexist/")
    assert tosfs.isdir(f"{bucket}/{temporary_workspace}/notexist/")

    tosfs.rmdir(f"{bucket}/{temporary_workspace}/notexist/{dir_name}")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}/notexist")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}/{dir_name}")
    tosfs.rmdir(f"{bucket}/{temporary_workspace}")


def test_put_file(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    temp_dir = create_temp_dir()
    file_name = f"{random_str()}.txt"
    lpath = f"{temp_dir}/{file_name}"
    rpath = f"{bucket}/{temporary_workspace}/{file_name}"

    with open(lpath, "w") as f:
        f.write("test content")

    assert not tosfs.exists(rpath)

    tosfs.put_file(lpath, rpath)
    assert tosfs.exists(rpath)

    bucket, key, _ = tosfs._split_path(rpath)
    assert (
        tosfs.tos_client.get_object(bucket, key).content.read().decode()
        == "test content"
    )

    with open(lpath, "w") as f:
        f.write("hello world")

    tosfs.put_file(lpath, rpath)
    assert (
        tosfs.tos_client.get_object(bucket, key).content.read().decode()
        == "hello world"
    )

    tosfs.rm_file(rpath)
    assert not tosfs.exists(rpath)

    tosfs.put(lpath, f"{bucket}/{temporary_workspace}")
    assert tosfs.exists(f"{bucket}/{temporary_workspace}/{file_name}")
    assert (
        tosfs.tos_client.get_object(bucket, f"{temporary_workspace}/{file_name}")
        .content.read()
        .decode()
        == "hello world"
    )

    with pytest.raises(IsADirectoryError):
        tosfs.put_file(temp_dir, f"{bucket}/{temporary_workspace}")

    with pytest.raises(FileNotFoundError):
        tosfs.put_file(f"/notexist/{random_str()}", rpath)

    with open(lpath, "wb") as f:
        f.write(b"a" * 1024 * 1024 * 6)

    # test mpu
    tosfs.put_file(lpath, rpath, chunksize=2 * 1024 * 1024)
    assert (
        tosfs.tos_client.get_object(bucket, key).content.read()
        == b"a" * 1024 * 1024 * 6
    )
    tosfs.rm_file(rpath)


def test_get_file(tosfs: TosFileSystem, bucket: str, temporary_workspace: str) -> None:
    file_name = random_str()
    file_content = "hello world"
    rpath = f"{bucket}/{temporary_workspace}/{file_name}"
    lpath = f"{tempfile.mkdtemp()}/{file_name}"
    assert not os.path.exists(lpath)

    bucket, key, _ = tosfs._split_path(rpath)
    tosfs.tos_client.put_object(bucket=bucket, key=key, content=file_content)

    tosfs.get_file(rpath, lpath)
    with open(lpath, "r") as f:
        assert f.read() == file_content

    with pytest.raises(FileNotFoundError):
        tosfs.get_file(f"{bucket}/{temporary_workspace}/nonexistent", lpath)

    tosfs.rm_file(rpath)
