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

from tosfs.core import TosFileSystem
from tosfs.utils import find_bucket_key


@pytest.mark.parametrize(
    ("input_str", "expected_output"),
    [
        ("bucket/key", ("bucket", "key")),
        ("bucket/", ("bucket", "")),
        ("tos://bucket/key", ("bucket", "key")),
        ("tos://bucket/", ("bucket", "")),
        ("/key", ("", "key")),
        ("bucket/key/with/slashes", ("bucket", "key/with/slashes")),
        ("bucket", ("bucket", "")),
        ("", ("", "")),
    ],
)
def test_find_bucket_key(
    tosfs: TosFileSystem, input_str: str, expected_output: str
) -> None:
    assert find_bucket_key(input_str) == expected_output
