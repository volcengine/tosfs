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

"""The core module of TOSFS."""
import logging
import os
from typing import List, Optional, Tuple, Union

import tos
from fsspec import AbstractFileSystem
from fsspec.utils import setup_logging as setup_logger
from tos.models import CommonPrefixInfo
from tos.models2 import ListedObject, ListedObjectVersion

from tosfs.exceptions import TosfsError
from tosfs.utils import find_bucket_key

# environment variable names
ENV_NAME_TOSFS_LOGGING_LEVEL = "TOSFS_LOGGING_LEVEL"

logger = logging.getLogger("tosfs")


def setup_logging() -> None:
    """Set up the logging configuration for TOSFS."""
    setup_logger(
        logger=logger,
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "INFO"),
    )


setup_logging()

logger.warning(
    "The tosfs's log level is set to be %s", logging.getLevelName(logger.level)
)


class TosFileSystem(AbstractFileSystem):
    """Tos file system.

    It's an implementation of AbstractFileSystem which is an
    abstract super-class for pythonic file-systems.
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        key: str = "",
        secret: str = "",
        region: Optional[str] = None,
        version_aware: bool = False,
        credentials_provider: Optional[object] = None,
        **kwargs: Union[str, bool, float, None],
    ) -> None:
        """Initialise the TosFileSystem."""
        self.tos_client = tos.TosClientV2(
            key,
            secret,
            endpoint_url,
            region,
            credentials_provider=credentials_provider,
        )
        self.version_aware = version_aware
        super().__init__(**kwargs)

    def ls(
        self,
        path: str,
        detail: bool = False,
        refresh: bool = False,
        versions: bool = False,
        **kwargs: Union[str, bool, float, None],
    ) -> Union[List[dict], List[str]]:
        """List objects under the given path.

        :param path: The path to list.
        :param detail: Whether to return detailed information.
        :param refresh: Whether to refresh the cache.
        :param versions: Whether to list object versions.
        :param kwargs: Additional arguments.
        :return: A list of objects under the given path.
        """
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._lsbuckets(refresh)
            return files if detail else sorted([o["name"] for o in files])

        files = self._lsdir(path, refresh, versions=versions)
        if not files and "/" in path:
            try:
                files = self._lsdir(
                    self._parent(path), refresh=refresh, versions=versions
                )
            except IOError:
                pass
            files = [
                o
                for o in files
                if o["name"].rstrip("/") == path and o["type"] != "directory"
            ]

        return files if detail else sorted([o["name"] for o in files])

    def _lsbuckets(self, refresh: bool = False) -> List[dict]:
        """List all buckets in the account.

        :param refresh: Whether to refresh the cache.
        :return: A list of buckets.
        """
        if "" not in self.dircache or refresh:
            try:
                resp = self.tos_client.list_buckets()
            except tos.exceptions.TosClientError as e:
                logger.error("Tosfs failed with client error: %s", e)
                raise e
            except tos.exceptions.TosServerError as e:
                logger.error("Tosfs failed with server error: %s", e)
                raise e
            except Exception as e:
                logger.error("Tosfs failed with unknown error: %s", e)
                raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

            buckets = [
                {
                    "Key": bucket.name,
                    "Size": 0,
                    "StorageClass": "BUCKET",
                    "size": 0,
                    "type": "directory",
                    "name": bucket.name,
                }
                for bucket in resp.buckets
            ]
            self.dircache[""] = buckets

        return self.dircache[""]

    def _lsdir(
        self,
        path: str,
        refresh: bool = False,
        max_items: int = 1000,
        delimiter: str = "/",
        prefix: str = "",
        versions: bool = False,
    ) -> List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]:
        """List objects in a bucket, here we use cache to improve performance.

        :param path: The path to list.
        :param refresh: Whether to refresh the cache.
        :param max_items: The maximum number of items to return, default is 1000.
        :param delimiter: The delimiter to use for grouping objects.
        :param prefix: The prefix to use for filtering objects.
        :param versions: Whether to list object versions.
        :return: A list of objects in the bucket.
        """
        bucket, key, _ = self._split_path(path)
        if not prefix:
            prefix = ""
        if key:
            prefix = key.lstrip("/") + "/" + prefix
        if path not in self.dircache or refresh or not delimiter or versions:
            logger.debug("Get directory listing for %s", path)
            dirs = []
            files = []
            for obj in self._listdir(
                bucket,
                max_items=max_items,
                delimiter=delimiter,
                prefix=prefix,
                versions=versions,
            ):
                if isinstance(obj, CommonPrefixInfo):
                    dirs.append(self._fill_common_prefix_info(obj, bucket))
                else:
                    files.append(self._fill_object_info(obj, bucket, versions))
            files += dirs

            if delimiter and files and not versions:
                self.dircache[path] = files
            return files
        return self.dircache[path]

    def _listdir(
        self,
        bucket: str,
        max_items: int = 1000,
        delimiter: str = "/",
        prefix: str = "",
        versions: bool = False,
    ) -> List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]:
        """List objects in a bucket.

        :param bucket: The bucket name.
        :param max_items: The maximum number of items to return, default is 1000.
        :param delimiter: The delimiter to use for grouping objects.
        :param prefix: The prefix to use for filtering objects.
        :param versions: Whether to list object versions.
        :return: A list of objects in the bucket.
        """
        if versions and not self.version_aware:
            raise ValueError(
                "versions cannot be specified if the filesystem is "
                "not version aware."
            )

        all_results = []
        is_truncated = True

        try:
            if self.version_aware:
                key_marker, version_id_marker = None, None
                while is_truncated:
                    resp = self.tos_client.list_object_versions(
                        bucket,
                        prefix,
                        delimiter=delimiter,
                        max_keys=max_items,
                        key_marker=key_marker,
                        version_id_marker=version_id_marker,
                    )
                    is_truncated = resp.is_truncated
                    all_results.extend(
                        resp.versions + resp.common_prefixes + resp.delete_markers
                    )
                    key_marker, version_id_marker = (
                        resp.next_key_marker,
                        resp.next_version_id_marker,
                    )
            else:
                continuation_token = ""
                while is_truncated:
                    resp = self.tos_client.list_objects_type2(
                        bucket,
                        prefix,
                        start_after=prefix,
                        delimiter=delimiter,
                        max_keys=max_items,
                        continuation_token=continuation_token,
                    )
                    is_truncated = resp.is_truncated
                    continuation_token = resp.next_continuation_token

                    all_results.extend(resp.contents + resp.common_prefixes)

            return all_results
        except tos.exceptions.TosClientError as e:
            logger.error(
                "Tosfs failed with client error, message:%s, cause: %s",
                e.message,
                e.cause,
            )
            raise e
        except tos.exceptions.TosServerError as e:
            logger.error("Tosfs failed with server error: %s", e)
            raise e
        except Exception as e:
            logger.error("Tosfs failed with unknown error: %s", e)
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _split_path(self, path: str) -> Tuple[str, str, Optional[str]]:
        """Normalise tos path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `tos://mybucket/path/to/file`

        Examples
        --------
        >>> split_path("tos://mybucket/path/to/file")
        ['mybucket', 'path/to/file', None]
        # pylint: disable=line-too-long
        >>> split_path("tos://mybucket/path/to/versioned_file?versionId=some_version_id")
        ['mybucket', 'path/to/versioned_file', 'some_version_id']

        """
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, "", None

        bucket, keypart = find_bucket_key(path)
        key, _, version_id = keypart.partition("?versionId=")
        return (
            bucket,
            key,
            version_id if self.version_aware and version_id else None,
        )

    @staticmethod
    def _fill_common_prefix_info(common_prefix: CommonPrefixInfo, bucket: str) -> dict:
        return {
            "name": common_prefix.prefix[:-1],
            "Key": "/".join([bucket, common_prefix.prefix]),
            "Size": 0,
            "type": "directory",
        }

    @staticmethod
    def _fill_object_info(
        obj: ListedObject, bucket: str, versions: bool = False
    ) -> dict:
        result = {
            "Key": f"{bucket}/{obj.key}",
            "size": obj.size,
            "name": f"{bucket}/{obj.key}",
            "type": "file",
        }
        if (
            isinstance(obj, ListedObjectVersion)
            and versions
            and obj.version_id
            and obj.version_id != "null"
        ):
            result["name"] += f"?versionId={obj.version_id}"
        return result
