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

from time import sleep

from tosfs.utils import random_str


def test_write_breakpoint_continuation(tosfs, bucket, temporary_workspace):
    file_name = f"{random_str()}.txt"
    first_part = random_str(10 * 1024 * 1024)
    second_part = random_str(10 * 1024 * 1024)

    with tosfs.open(f"{bucket}/{temporary_workspace}/{file_name}", "w") as f:
        f.write(first_part)
        # mock a very long block(business processing or network issue)
        sleep(60 * 15)
        f.write(second_part)

    assert tosfs.info(f"{bucket}/{temporary_workspace}/{file_name}")["size"] == len(
        first_part + second_part
    )

    with tosfs.open(f"{bucket}/{temporary_workspace}/{file_name}", "r") as f:
        assert f.read() == first_part + second_part


def test_read_breakpoint_continuation(tosfs, bucket, temporary_workspace):
    file_name = f"{random_str()}.txt"
    first_part = random_str(10 * 1024 * 1024)
    second_part = random_str(10 * 1024 * 1024)

    with tosfs.open(f"{bucket}/{temporary_workspace}/{file_name}", "w") as f:
        f.write(first_part)
        f.write(second_part)

    with tosfs.open(f"{bucket}/{temporary_workspace}/{file_name}", "r") as f:
        read_first_part = f.read(10 * 1024 * 1024)
        assert read_first_part == first_part
        # mock a very long block(business processing or network issue)
        sleep(60 * 15)
        read_second_part = f.read(10 * 1024 * 1024)
        assert read_second_part == second_part
        assert read_first_part + read_second_part == first_part + second_part
