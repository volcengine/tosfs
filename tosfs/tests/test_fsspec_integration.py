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

import fsspec
from fsspec.registry import known_implementations
from tos import EnvCredentialsProvider


def test_fssepc_register():
    fsspec.register_implementation("tos", "tosfs.TosFileSystem")

    tosfs, _ = fsspec.core.url_to_fs(
        "tos://",
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )
    assert len(tosfs.ls("")) > 0


def test_set_known_implementations():
    known_implementations["tos"] = {"class": "tosfs.core.TosFileSystem"}

    tosfs, _ = fsspec.core.url_to_fs(
        "tos://",
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )
    assert len(tosfs.ls("")) > 0
