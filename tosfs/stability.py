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

"""The module contains utility functions for the tosfs stability."""

import time
from typing import Any, Optional, Tuple

import requests
from requests.exceptions import (
    ConnectTimeout,
    HTTPError,
    ProxyError,
    ReadTimeout,
    RetryError,
    SSLError,
    StreamConsumedError,
    Timeout,
)
from tos.exceptions import TosClientError, TosError, TosServerError

from tosfs.exceptions import TosfsError

TOS_SERVER_RETRYABLE_STATUS_CODES = {
    "409",  # CONFLICT
    "429",  # TOO_MANY_REQUESTS
    "500",  # INTERNAL_SERVER_ERROR
}

TOS_SERVER_NOT_RETRYABLE_CONFLICT_ERROR_CODES = {
    "0026-00000013",  # DELETE_NON_EMPTY_DIR
    "0026-00000020",  # LOCATED_UNDER_A_FILE
    "0026-00000021",  # COPY_BETWEEN_DIR_AND_FILE
    "0026-00000022",  # PATH_LOCK_CONFLICT
    "0026-00000025",  # RENAME_TO_AN_EXISTED_DIR
    "0026-00000026",  # RENAME_TO_SUB_DIR
    "0026-00000027",  # RENAME_BETWEEN_DIR_AND_FILE
    "0017-00000208",  # APPEND_OFFSET_NOT_MATCHED
    "0017-00000209",  # APPEND_NOT_APPENDABLE
}

TOS_CLIENT_RETRYABLE_EXCEPTIONS = {
    HTTPError,
    requests.ConnectionError,
    ProxyError,
    SSLError,
    Timeout,
    ConnectTimeout,
    ReadTimeout,
    StreamConsumedError,
    RetryError,
    InterruptedError,
    ConnectionResetError,
    ConnectionError,
}

MAX_RETRY_NUM = 20


def retryable_func(
    func: Any,
    *,
    args: Tuple[Any, ...] = (),
    kwargs: Optional[Any] = None,
    max_retry_num: int = MAX_RETRY_NUM,
) -> Any:
    """Retry a function in case of catch errors."""
    if kwargs is None:
        kwargs = {}

    attempt = 0

    while attempt < max_retry_num:
        attempt += 1
        try:
            return func(*args, **kwargs)
        except TosError as e:
            from tosfs.core import logger

            if attempt >= max_retry_num:
                logger.error("Retry exhausted after %d times.", max_retry_num)
                raise e

            if is_retryable_exception(e):
                logger.warn("Retry TOS request in the %d times, error: %s", attempt, e)
                try:
                    time.sleep(min(1.7**attempt * 0.1, 15))
                except InterruptedError as ie:
                    raise TosfsError(f"Request {func} interrupted.") from ie
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"{e}") from e


def is_retryable_exception(e: TosError) -> bool:
    """Check if the exception is retryable."""
    return _is_retryable_tos_server_exception(e) or _is_retryable_tos_client_exception(
        e
    )


def _is_retryable_tos_server_exception(e: TosError) -> bool:
    return (
        isinstance(e, TosServerError)
        and e.status_code in TOS_SERVER_RETRYABLE_STATUS_CODES
        # exclude some special error code under 409(conflict) status code
        # let it fast fail
        and e.code not in TOS_SERVER_NOT_RETRYABLE_CONFLICT_ERROR_CODES
    )


def _is_retryable_tos_client_exception(e: TosError) -> bool:
    return isinstance(e, TosClientError) and any(
        isinstance(e.cause, excp) for excp in TOS_CLIENT_RETRYABLE_EXCEPTIONS
    )
