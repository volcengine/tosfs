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

from tos import EnvCredentialsProvider

from tosfs.core import TosFileSystem
import tarfile

def streaming_read_tar_from_tos(bucket_name, tar_key):
    tosfs = TosFileSystem(
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )

    with tosfs.open(f'tos://{bucket_name}/{tar_key}', 'rb') as tos_file:
        with tarfile.open(fileobj=tos_file, mode='r|*') as tar:
            for member in tar:
                if member.isfile():
                    file_obj = tar.extractfile(member)
                    if file_obj is not None:
                        file_content = file_obj.read()
                        print(f'Read file {member.name} with size {len(file_content)} bytes')

if __name__ == "__main__":
    bucket_name = ''
    tar_path = ''
    streaming_read_tar_from_tos(bucket_name, tar_path)
