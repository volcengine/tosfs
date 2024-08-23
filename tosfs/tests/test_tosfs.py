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

import pytest
from tos.exceptions import TosServerError

from tosfs.core import TosFileSystem
from tosfs.exceptions import TosfsError
from tosfs.utils import random_path


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
    file_name = random_path()
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

    file_name = random_path()
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
