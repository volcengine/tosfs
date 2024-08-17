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

import logging
import os

from tosfs.core import ENV_NAME_TOSFS_LOGGING_LEVEL, setup_logging


def test_logging_level_debug() -> None:
    # Set the environment variable to DEBUG
    os.environ[ENV_NAME_TOSFS_LOGGING_LEVEL] = "DEBUG"

    # Re-setup logging to apply the new environment variable
    setup_logging()

    # Get the logger and check its level
    logger = logging.getLogger("tosfs")
    assert logger.level == logging.DEBUG
