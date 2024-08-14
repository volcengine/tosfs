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

import pytest
from tos import EnvCredentialsProvider

from tosfs.core import TosFileSystem
from tosfs.utils import random_path


@pytest.fixture(scope="module")
def tosfs_env_prepare():
    if "TOS_ACCESS_KEY" not in os.environ:
        raise EnvironmentError(
            "Can not find TOS_ACCESS_KEY in environment variables."
        )
    if "TOS_SECRET_KEY" not in os.environ:
        raise EnvironmentError(
            "Can not find TOS_SECRET_KEY in environment variables."
        )


@pytest.fixture(scope="module")
def tosfs(tosfs_env_prepare):
    tosfs = TosFileSystem(
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )
    yield tosfs


@pytest.fixture(scope="module")
def bucket():
    yield os.environ.get("TOS_BUCKET", "proton-ci")


@pytest.fixture(autouse=True)
def temporary_workspace(tosfs, bucket):
    workspace = random_path()
    # currently, make dir via purely tos python client,
    # will replace with tosfs.mkdir in the future
    tosfs.tos_client.put_object(bucket=bucket, key=f"{workspace}/")
    yield workspace
    # currently, remove dir via purely tos python client,
    # will replace with tosfs.rmdir in the future
    tosfs.tos_client.delete_object(bucket=bucket, key=f"{workspace}/")
