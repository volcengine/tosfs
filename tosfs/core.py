# ByteDance Volcengine EMR, Copyright 2022.
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

"""
The core module of TOSFS.
"""
import logging
import os
import re
from typing import Optional, Tuple

import tos
from fsspec import AbstractFileSystem
from fsspec.utils import setup_logging as setup_logger

# environment variable names
ENV_NAME_TOSFS_LOGGING_LEVEL = "TOSFS_LOGGING_LEVEL"

logger = logging.getLogger("tosfs")


def setup_logging():
    """
    Set up the logging configuration for TOSFS.
    """
    setup_logger(
        logger=logger,
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "INFO"),
    )


setup_logging()

logger.warning(
    "The tosfs's log level is set to be %s", logging.getLevelName(logger.level)
)


class TosFileSystem(AbstractFileSystem):
    """
    Tos file system. An implementation of AbstractFileSystem which is an
    abstract super-class for pythonic file-systems.
    """

    def __init__(
        self,
        endpoint_url=None,
        key="",
        secret="",
        region=None,
        version_aware=False,
        credentials_provider=None,
    ):
        self.tos_client = tos.TosClientV2(
            key,
            secret,
            endpoint_url,
            region,
            credentials_provider=credentials_provider,
        )
        self.version_aware = version_aware
        super().__init__()

    def ls(self, path, detail=False, refresh=False, versions=False, **kwargs):
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._lsbuckets(refresh)
        else:
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
                    if o["name"].rstrip("/") == path
                    and o["type"] != "directory"
                ]
                if not files:
                    raise FileNotFoundError(path)
            if detail:
                return files
        return files if detail else sorted([o.name for o in files])

    def _lsbuckets(self, refresh=False):
        if "" not in self.dircache or refresh:
            try:
                resp = self.tos_client.list_buckets()
            except tos.exceptions.TosClientError as e:
                logger.error(
                    "Tosfs failed with client error, message:%s, cause: %s",
                    e.message,
                    e.cause,
                )
                return []
            except tos.exceptions.TosServerError as e:
                logger.error("Tosfs failed with server error: %s", e)
                return []
            except Exception as e:
                logger.error("Tosfs failed with unknown error: %s", e)
                return []

            self.dircache[""] = resp.buckets
            return resp.buckets
        return self.dircache[""]

    def _lsdir(
        self,
        path,
        refresh=False,
        max_items=None,
        delimiter="/",
        prefix="",
        versions=False,
    ):
        bucket, key, _ = self.split_path(path)
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
                if obj["type"] == "directory":
                    dirs.append(obj)
                else:
                    files.append(obj)
            files += dirs

            if delimiter and files and not versions:
                self.dircache[path] = files
            return files
        return self.dircache[path]

    def _listdir(
        self, bucket, max_items=None, delimiter="/", prefix="", versions=False
    ):
        if versions and not self.version_aware:
            raise ValueError(
                "versions cannot be specified if the filesystem is "
                "not version aware."
            )

        paging_fetch = max_items is None
        pag_size = 50

        all_results = []
        start_after = None
        version_id_marker = None

        try:
            if self.version_aware:
                if paging_fetch:
                    while True:
                        resp = self.tos_client.list_object_versions(
                            bucket,
                            prefix,
                            delimiter=delimiter,
                            key_marker=start_after,
                            version_id_marker=version_id_marker,
                            max_keys=pag_size,
                        )
                        if not resp.versions:
                            break
                        all_results.extend(resp.versions)
                        start_after = resp.versions[-1].key
                        version_id_marker = resp.versions[-1].version_id
            else:
                if paging_fetch:
                    while True:
                        resp = self.tos_client.list_objects_type2(
                            bucket,
                            prefix,
                            delimiter=delimiter,
                            start_after=start_after,
                            max_keys=pag_size,
                        )
                        if not resp.contents:
                            break
                        all_results.extend(resp.contents)
                        start_after = resp.contents[-1].key
                else:
                    resp = self.tos_client.list_objects_type2(
                        bucket,
                        prefix,
                        start_after=start_after,
                        max_keys=max_items,
                    )
                    all_results.extend(resp.contents)

            return all_results
        except tos.exceptions.TosClientError as e:
            logger.error(
                "Tosfs failed with client error, message:%s, cause: %s",
                e.message,
                e.cause,
            )
            return []
        except tos.exceptions.TosServerError as e:
            logger.error("Tosfs failed with server error: %s", e)
            return []
        except Exception as e:
            logger.error("Tosfs failed with unknown error: %s", e)
            return []

    def split_path(self, path) -> Tuple[str, str, Optional[str]]:
        """
        Normalise tos path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `tos://mybucket/path/to/file`

        Examples
        --------
        >>> split_path("tos://mybucket/path/to/file")
        ['mybucket', 'path/to/file', None]
        # pylint: disable=line-too-long
        >>> split_path("tos://mybucket/path/to/versioned_file?versionId=some_version_id")  # noqa: E501
        ['mybucket', 'path/to/versioned_file', 'some_version_id']
        """
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, "", None

        bucket, keypart = self._find_bucket_key(path)
        key, _, version_id = keypart.partition("?versionId=")
        return (
            bucket,
            key,
            version_id if self.version_aware and version_id else None,
        )

    def _find_bucket_key(self, tos_path):
        """
        This is a helper function that given an tos path such that the path
        is of the form: bucket/key
        It will return the bucket and the key represented by the tos path
        """

        bucket_format_list = [
            re.compile(
                r"^(?P<bucket>:tos:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?"  # noqa: E501
                r"(?P<key>.*)$"
            ),
            re.compile(
                r"^(?P<bucket>:tos-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
                # pylint: disable=line-too-long
                r"[a-zA-Z0-9\-]{1,63}[/:](bucket|accesspoint)[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"  # noqa: E501
            ),
            re.compile(
                r"^(?P<bucket>:tos-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
                r"[a-zA-Z0-9\-]{1,63}[/:]bucket[/:]"
                r"[a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
            ),
            re.compile(
                r"^(?P<bucket>:tos-object-lambda:[a-z\-0-9]+:[0-9]{12}:"
                r"accesspoint[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
            ),
        ]
        for bucket_format in bucket_format_list:
            match = bucket_format.match(tos_path)
            if match:
                return match.group("bucket"), match.group("key")
        tos_components = tos_path.split("/", 1)
        bucket = tos_components[0]
        tos_key = ""
        if len(tos_components) > 1:
            tos_key = tos_components[1]
        return bucket, tos_key

    @staticmethod
    def _fill_info(f, bucket, versions=False):
        f["size"] = f["Size"]
        f["Key"] = "/".join([bucket, f["Key"]])
        f["name"] = f["Key"]
        version_id = f.get("VersionId")
        if versions and version_id and version_id != "null":
            f["name"] += f"?versionId={version_id}"
