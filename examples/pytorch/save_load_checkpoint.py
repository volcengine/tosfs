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

# packages:
# pip install torch
# pip install torchvision

import os

import torch
import torchvision
from tos import EnvCredentialsProvider

from tosfs.core import TosFileSystem

if __name__ == '__main__':
    tosfs = TosFileSystem(
        endpoint_url=os.environ.get("TOS_ENDPOINT"),
        region=os.environ.get("TOS_REGION"),
        credentials_provider=EnvCredentialsProvider(),
    )

    model = torchvision.models.resnet18()

    # save
    with tosfs.open('bucket/checkpoint/epoch1.ckpt', 'wb') as writer:
        torch.save(model.state_dict(), writer)

    # load
    with tosfs.open('bucket/checkpoint/epoch1.ckpt', 'rb') as reader:
        stat_dict = torch.load(reader)

    model.load_state_dict(stat_dict)
