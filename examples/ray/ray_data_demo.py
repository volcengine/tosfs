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

# prepare:
# pip install "ray[data]"

import ray
import logging
from tosfs.core import TosFileSystem

# csv file path
input_csv_path = "tos://your-bucket/input.csv"
output_csv_path = "tos://your-bucket/output.csv"

# TOS AK
ENV_AK = 'your ak'
# TOS SK
ENV_SK = 'your sk'
# TOS ENDPOINT
TOS_ENV_ENDPOINT = 'https://tos-cn-beijing.volces.com'

ray.init(runtime_env={
    "env_vars": {"RAY_LOGGING_LEVEL": "INFO"},
    "pip": ["pyarrow", "tosfs"]
})

logging.basicConfig(level=logging.INFO)

tos_fs = TosFileSystem(
    key=ENV_AK,
    secret=ENV_SK,
    endpoint_url=TOS_ENV_ENDPOINT,
    region='cn-beijing',
    socket_timeout=60,
    connection_timeout=60,
    max_retry_num=30
)


if __name__ == "__main__":
    # init: write a file to tos
    with tos_fs.open(input_csv_path, 'w') as f:
        f.write("id,name,age\n1,John Doe,30\n2,Jane Smith,25\n3,Bob Johnson,40\n")

    ds = ray.data.read_csv(input_csv_path, filesystem=tos_fs)

    # processing data
    #ds = ds.map_batches(your_processing_function)

    ds.repartition(1).write_csv(output_csv_path, filesystem=tos_fs)


