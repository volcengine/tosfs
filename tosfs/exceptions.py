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

"""It contains exceptions definition for the tosfs package."""
from typing import Optional


class TosfsError(Exception):
    """Base class for all tosfs exceptions."""

    def __init__(self, msg: str, cause: Optional[Exception] = None):
        """Initialize the base class for all exceptions in the tosfs package."""
        super().__init__(msg, cause)
        self.message = msg
        self.cause = cause

    def __str__(self) -> str:
        """Return the string representation of the exception."""
        error = {"message": self.message, "case": str(self.cause)}
        return str(error)


class TosfsCertificationError(TosfsError):
    """Exception class for certification related exception."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        """Initialize the exception class for certification related exception."""
        super().__init__(message, cause)
