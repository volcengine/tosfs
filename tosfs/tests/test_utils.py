# ByteDance Volcengine EMR, Copyright 2022.
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


@pytest.mark.parametrize(
    "input_str, expected_output",
    [
        ("bucket/key", ("bucket", "key")),
        ("bucket/", ("bucket", "")),
        ("/key", ("", "key")),
        ("bucket/key/with/slashes", ("bucket", "key/with/slashes")),
        ("bucket", ("bucket", "")),
        ("", ("", "")),
    ],
)
def test_find_bucket_key(tosfs, input_str, expected_output):
    assert tosfs._find_bucket_key(input_str) == expected_output
