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
from tosfs.core import TosFileSystem

# init TosFileSystem
fs = TosFileSystem(
    endpoint_url="https://your-tos-endpoint",
    key="your-access-key",
    secret="your-secret-key",
    region="your-region"
)

local_file_path = "localfile.txt"
remote_file_path = "tos://your-bucket/remote_file.txt"

# create a local file to upload
with open(local_file_path, "w") as f:
    f.write("Hello TOSFS.")

# upload to tos
fs.put_file(local_file_path, remote_file_path)
print(f"Uploaded {local_file_path} to {remote_file_path}")

# download from tos
downloaded_file_path = "downloaded_file.txt"
fs.get_file(remote_file_path, downloaded_file_path)
print(f"Downloaded {remote_file_path} to {downloaded_file_path}")

# read content from downloaded local file
with open(downloaded_file_path, "r") as f:
    content = f.read()
    print(f"Content of {downloaded_file_path}: {content}")

# delete tos file
fs.rm(remote_file_path)

# write to tos
with fs.open(remote_file_path, "w") as f:
    f.write("Hello TOSFS.")

# read from tos
with fs.open(remote_file_path, "r") as f:
    tos_content = f.read()
    print(f"Content of {remote_file_path}: {tos_content}")

# clean local files
os.remove(local_file_path)
os.remove(downloaded_file_path)