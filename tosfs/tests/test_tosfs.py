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
import unittest
from unittest.mock import MagicMock

import pytest
from tos.exceptions import TosServerError

from tosfs.core import TosFileSystem
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
    assert temporary_workspace in tosfs.ls(bucket, detail=False)
    detailed_list = tosfs.ls(bucket, detail=True)
    assert detailed_list
    for item in detailed_list:
        if item["name"] == temporary_workspace:
            assert item["type"] == "directory"
            break
    else:
        raise AssertionError(
            f"Directory {temporary_workspace} not found in {detailed_list}"
        )

    assert tosfs.ls(f"{bucket}/{temporary_workspace}", detail=False) == []
    assert tosfs.ls(f"{bucket}/{temporary_workspace}/nonexistent", detail=False) == []


def test_ls_cache(tosfs: TosFileSystem, bucket: str) -> None:
    with unittest.mock.patch.object(
        tosfs.tos_client,
        "list_objects_type2",
        return_value=MagicMock(
            is_truncated=False,
            contents=[MagicMock(key="mock_key", size=123)],
            common_prefixes=[],
            next_continuation_token=None,
        ),
    ):
        # Call ls method and get result from server
        tosfs.ls(bucket, detail=False, refresh=True)
        # Get result from cache
        tosfs.ls(bucket, detail=False, refresh=False)

        # Verify that list_objects_type2 was called only once
        assert tosfs.tos_client.list_objects_type2.call_count == 1


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
