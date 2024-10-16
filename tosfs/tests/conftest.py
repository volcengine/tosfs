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

import os
from typing import Any, Generator

import fsspec
import pytest
from fsspec.registry import known_implementations
from tos import EnvCredentialsProvider

from tosfs.core import TosFileSystem, logger
from tosfs.utils import random_str


@pytest.fixture(scope="module")
def _tosfs_env_prepare() -> None:
    if "TOS_ACCESS_KEY" not in os.environ:
        raise EnvironmentError("Can not find TOS_ACCESS_KEY in environment variables.")
    if "TOS_SECRET_KEY" not in os.environ:
        raise EnvironmentError("Can not find TOS_SECRET_KEY in environment variables.")


@pytest.fixture(scope="module")
def tosfs(_tosfs_env_prepare: None) -> TosFileSystem:
    tosfs = TosFileSystem(
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
        max_retry_num=1000,
        connection_timeout=300,
        socket_timeout=300,
        multipart_size=4 << 20,
        multipart_threshold=4 << 20,
    )
    return tosfs


@pytest.fixture(scope="module")
def fsspecfs(_tosfs_env_prepare: None) -> Any:
    known_implementations["tos"] = {"class": "tosfs.TosFileSystem"}

    fsspecfs, _ = fsspec.core.url_to_fs(
        "tos://",
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )
    return fsspecfs


@pytest.fixture(scope="module")
def bucket() -> str:
    return os.environ.get("TOS_BUCKET", "proton-ci")


@pytest.fixture(autouse=True)
def temporary_workspace(
    tosfs: TosFileSystem, bucket: str
) -> Generator[str, None, None]:
    workspace = random_str()
    tosfs.mkdirs(f"{bucket}/{workspace}/")
    yield workspace
    try:
        tosfs.rm(f"{bucket}/{workspace}/", recursive=True)
    except Exception as e:
        logger.error(f"Ignore exception : {e}.")
    assert not tosfs.exists(f"{bucket}/{workspace}/")
