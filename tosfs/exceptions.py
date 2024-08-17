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
"""iT contains exceptions definition for the tosfs package."""


class TosfsError(Exception):
    """Base class for all tosfs exceptions."""

    def __init__(self, message: str):
        """Initialize the base class for all exceptions in the tosfs package."""
        super().__init__(message)
