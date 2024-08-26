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
from typing import Any, List, Optional, Tuple, Union

import tos
from fsspec import AbstractFileSystem
from fsspec.utils import setup_logging as setup_logger
from tos.models import CommonPrefixInfo
from tos.models2 import ListedObject, ListedObjectVersion

from tosfs.consts import TOS_SERVER_RESPONSE_CODE_NOT_FOUND
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
        versions: bool = False,
        **kwargs: Union[str, bool, float, None],
    ) -> Union[List[dict], List[str]]:
        """List objects under the given path.

        Parameters
        ----------
        path : str
            The path to list.
        detail : bool, optional
            Whether to return detailed information (default is False).
        versions : bool, optional
            Whether to list object versions (default is False).
        **kwargs : dict, optional
            Additional arguments.

        Returns
        -------
        Union[List[dict], List[str]]
            A list of objects under the given path. If `detail` is True,
            returns a list of dictionaries with detailed information.
            Otherwise, returns a list of object names.

        Raises
        ------
        IOError
            If there is an error accessing the parent directory.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.ls("mybucket")
        ['mybucket/file1', 'mybucket/file2']
        >>> fs.ls("mybucket", detail=True)
        [{'name': 'mybucket/file1', 'size': 123, 'type': 'file'},
        {'name': 'mybucket/file2', 'size': 456, 'type': 'file'}]

        """
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._lsbuckets()
            return files if detail else sorted([o["name"] for o in files])

        files = self._lsdir(path, versions=versions)
        if not files and "/" in path:
            try:
                files = self._lsdir(self._parent(path), versions=versions)
            except IOError:
                pass
            files = [
                o
                for o in files
                if o["name"].rstrip("/") == path and o["type"] != "directory"
            ]

        return files if detail else sorted([o["name"] for o in files])

    def info(
        self,
        path: str,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        version_id: Optional[str] = None,
    ) -> dict:
        """Give details of entry at path.

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.

        """
        if path in ["/", ""]:
            return {"name": path, "size": 0, "type": "directory"}
        path = self._strip_protocol(path)
        bucket, key, path_version_id = self._split_path(path)
        fullpath = "/".join((bucket, key))

        if version_id is not None and not self.version_aware:
            raise ValueError(
                "version_id cannot be specified due to the "
                "filesystem is not support version aware."
            )

        if not key:
            return self._bucket_info(bucket)

        if info := self._object_info(bucket, key, version_id):
            return info

        return self._try_dir_info(bucket, key, path, fullpath)

    def rmdir(self, path: str) -> None:
        """Remove a directory if it is empty.

        Parameters
        ----------
        path : str
            The path of the directory to remove. The path should be in the format
            `tos://bucket/path/to/directory`.

        Raises
        ------
        FileNotFoundError
            If the directory does not exist.
        NotADirectoryError
            If the path is not a directory.
        TosfsError
            If the directory is not empty,
             or the path is a bucket.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.rmdir("tos://mybucket/mydir/")

        """
        path = self._strip_protocol(path).rstrip("/") + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            raise TosfsError("Cannot remove a bucket using rmdir api.")

        if not self.exists(path):
            raise FileNotFoundError(f"Directory {path} not found.")

        if not self.isdir(path):
            raise NotADirectoryError(f"{path} is not a directory.")

        if len(self._listdir(bucket, max_items=1, prefix=key.rstrip("/") + "/")) > 0:
            raise TosfsError(f"Directory {path} is not empty.")

        try:
            self.tos_client.delete_object(bucket, key.rstrip("/") + "/")
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def touch(self, path: str, truncate: bool = True, **kwargs: Any) -> None:
        """Create an empty file at the given path.

        Parameters
        ----------
        path : str
            The path of the file to create.
        truncate : bool, optional
            Whether to truncate the file if it already exists (default is True).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileExistsError
            If the file already exists and `truncate` is False.
        TosfsError
            If there is an unknown error while creating the file.
        tos.exceptions.TosClientError
            If there is a client error while creating the file.
        tos.exceptions.TosServerError
            If there is a server error while creating the file.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.touch("tos://mybucket/myfile")

        """
        path = self._strip_protocol(path)
        bucket, key, _ = self._split_path(path)

        if not truncate and self.exists(path):
            raise FileExistsError(f"File {path} already exists.")

        try:
            self.tos_client.put_object(bucket, key)
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def isdir(self, path: str) -> bool:
        """Check if the path is a directory.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        bool
            True if the path is a directory, False otherwise.

        Raises
        ------
        TosClientError
            If there is a client error while accessing the path.
        TosServerError
            If there is a server error while accessing the path.
        TosfsError
            If there is an unknown error while accessing the path.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.isdir("tos://mybucket/mydir/")

        """
        path = self._strip_protocol(path).rstrip("/") + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            return False

        key = key.rstrip("/") + "/"

        try:
            self.tos_client.head_object(bucket, key)
            return True
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                return False
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def isfile(self, path: str) -> bool:
        """Check if the path is a file.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        bool
            True if the path is a file, False otherwise.

        """
        if path.endswith("/"):
            return False

        bucket, key, _ = self._split_path(path)
        try:
            # Attempt to get the object metadata
            self.tos_client.head_object(bucket, key)
            return True
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                return False
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _bucket_info(self, bucket: str) -> dict:
        """Get the information of a bucket.

        Parameters
        ----------
        bucket : str
            The name of the bucket.

        Returns
        -------
        dict
            A dictionary containing the bucket information with the following keys:
            - 'Key': The bucket name.
            - 'Size': The size of the bucket (always 0).
            - 'StorageClass': The storage class of the bucket (always 'BUCKET').
            - 'size': The size of the bucket (always 0).
            - 'type': The type of the bucket (always 'directory').
            - 'name': The bucket name.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while accessing the bucket.
        tos.exceptions.TosServerError
            If there is a server error while accessing the bucket.
        FileNotFoundError
            If the bucket does not exist.
        TosfsError
            If there is an unknown error while accessing the bucket.

        """
        try:
            self.tos_client.head_bucket(bucket)
            return self._fill_bucket_info(bucket)
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                raise FileNotFoundError(bucket) from e
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _object_info(
        self, bucket: str, key: str, version_id: Optional[str] = None
    ) -> dict:
        """Get the information of an object.

        Parameters
        ----------
        bucket : str
            The bucket name.
        key : str
            The object key.
        version_id : str, optional
            The version id of the object (default is None).

        Returns
        -------
        dict
            A dictionary containing the object information with the following keys:
            - 'ETag': The entity tag of the object.
            - 'LastModified': The last modified date of the object.
            - 'size': The size of the object in bytes.
            - 'name': The full path of the object.
            - 'type': The type of the object (always 'file').
            - 'StorageClass': The storage class of the object.
            - 'VersionId': The version id of the object.
            - 'ContentType': The content type of the object.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while accessing the object.
        tos.exceptions.TosServerError
            If there is a server error while accessing the object.
        TosfsError
            If there is an unknown error while accessing the object.

        """
        try:
            out = self.tos_client.head_object(bucket, key, version_id=version_id)
            return {
                "ETag": out.etag or "",
                "LastModified": out.last_modified or "",
                "size": out.content_length or 0,
                "name": "/".join((bucket, key)),
                "type": "file",
                "StorageClass": out.storage_class or "STANDARD",
                "VersionId": out.version_id or "",
                "ContentType": out.content_type or "",
            }
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                pass
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

        return {}

    def _try_dir_info(self, bucket: str, key: str, path: str, fullpath: str) -> dict:
        try:
            # We check to see if the path is a directory by attempting to list its
            # contexts. If anything is found, it is indeed a directory
            out = self.tos_client.list_objects_type2(
                bucket,
                prefix=key.rstrip("/") + "/" if key else "",
                delimiter="/",
                max_keys=1,
            )

            if out.key_count > 0 or out.contents or out.common_prefixes:
                return {
                    "name": fullpath,
                    "type": "directory",
                    "size": 0,
                    "StorageClass": "DIRECTORY",
                }

            raise FileNotFoundError(path)
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists in the TOS.

        Parameters
        ----------
        path : str
            The path to check for existence.
        **kwargs : Any, optional
            Additional arguments if needed in the future.

        Returns
        -------
        bool
            True if the path exists, False otherwise.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while checking the path.
        tos.exceptions.TosServerError
            If there is a server error while checking the path.
        TosfsError
            If there is an unknown error while checking the path.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.exists("tos://bucket/to/file")
        True
        >>> fs.exists("tos://mybucket/nonexistentfile")
        False

        """
        if path in ["", "/"]:
            # the root always exists
            return True

        path = self._strip_protocol(path)
        bucket, key, version_id = self._split_path(path)
        # if the path is a bucket
        if not key:
            return self._exists_bucket(bucket)
        else:
            object_exists = self._exists_object(bucket, key, path, version_id)
            if not object_exists:
                return self._exists_object(
                    bucket, key.rstrip("/") + "/", path, version_id
                )
            return object_exists

    def _exists_bucket(self, bucket: str) -> bool:
        """Check if a bucket exists in the TOS.

        Parameters
        ----------
        bucket : str
            The name of the bucket to check for existence.

        Returns
        -------
        bool
            True if the bucket exists, False otherwise.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while checking the bucket.
        tos.exceptions.TosServerError
            If there is a server error while checking the bucket.
        TosfsError
            If there is an unknown error while checking the bucket.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs._exists_bucket("mybucket")
        True
        >>> fs._exists_bucket("nonexistentbucket")
        False

        """
        try:
            self.tos_client.head_bucket(bucket)
            return True
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                return False
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _exists_object(
        self, bucket: str, key: str, path: str, version_id: Optional[str] = None
    ) -> bool:
        """Check if an object exists in the TOS.

        Parameters
        ----------
        bucket : str
            The name of the bucket.
        key : str
            The key of the object.
        path : str
            The full path of the object.
        version_id : str, optional
            The version ID of the object (default is None).

        Returns
        -------
        bool
            True if the object exists, False otherwise.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while checking the object.
        tos.exceptions.TosServerError
            If there is a server error while checking the object.
        TosfsError
            If there is an unknown error while checking the object.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs._exists_object("mybucket", "myfile", "tos://mybucket/myfile")
        True
        >>> fs._exists_object("mybucket", "nonexistentfile", "tos://mybucket/nonexistentfile")
        False

        """
        try:
            self.tos_client.head_object(bucket, key)
            return True
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            if e.status_code == TOS_SERVER_RESPONSE_CODE_NOT_FOUND:
                return False
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _lsbuckets(self) -> List[dict]:
        """List all buckets in the account.

        Returns
        -------
        List[dict]
            A list of dictionaries,
            each containing information about a bucket with the following keys:
            - 'Key': The bucket name.
            - 'Size': The size of the bucket (always 0).
            - 'StorageClass': The storage class of the bucket (always 'BUCKET').
            - 'size': The size of the bucket (always 0).
            - 'type': The type of the bucket (always 'directory').
            - 'name': The bucket name.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while listing the buckets.
        tos.exceptions.TosServerError
            If there is a server error while listing the buckets.
        TosfsError
            If there is an unknown error while listing the buckets.

        """
        try:
            resp = self.tos_client.list_buckets()
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

        return [self._fill_bucket_info(bucket.name) for bucket in resp.buckets]

    def _lsdir(
        self,
        path: str,
        max_items: int = 1000,
        delimiter: str = "/",
        prefix: str = "",
        versions: bool = False,
    ) -> List[dict]:
        """List objects in a directory.

        Parameters
        ----------
        path : str
            The path to list.
        max_items : int, optional
            The maximum number of items to return (default is 1000).
        delimiter : str, optional
            The delimiter to use for grouping objects (default is '/').
        prefix : str, optional
            The prefix to use for filtering objects (default is '').
        versions : bool, optional
            Whether to list object versions (default is False).

        Returns
        -------
        List[dict]
            A list of objects in the directory.

        Raises
        ------
        ValueError
            If `versions` is specified but the filesystem is not version aware.
        tos.exceptions.TosClientError
            If there is a client error while listing the objects.
        tos.exceptions.TosServerError
            If there is a server error while listing the objects.
        TosfsError
            If there is an unknown error while listing the objects.

        """
        bucket, key, _ = self._split_path(path)
        if not prefix:
            prefix = ""
        if key:
            prefix = key.lstrip("/") + "/" + prefix

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

        return files

    def _listdir(
        self,
        bucket: str,
        max_items: int = 1000,
        delimiter: str = "/",
        prefix: str = "",
        versions: bool = False,
    ) -> List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]:
        """List objects in a bucket.

        Parameters
        ----------
        bucket : str
            The bucket name.
        max_items : int, optional
            The maximum number of items to return (default is 1000).
        delimiter : str, optional
            The delimiter to use for grouping objects (default is '/').
        prefix : str, optional
            The prefix to use for filtering objects (default is '').
        versions : bool, optional
            Whether to list object versions (default is False).

        Returns
        -------
        List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]
            A list of objects in the bucket.
            The list may contain `CommonPrefixInfo` for directories,
            `ListedObject` for files, and `ListedObjectVersion` for versioned objects.

        Raises
        ------
        ValueError
            If `versions` is specified but the filesystem is not version aware.
        tos.exceptions.TosClientError
            If there is a client error while listing the objects.
        tos.exceptions.TosServerError
            If there is a server error while listing the objects.
        TosfsError
            If there is an unknown error while listing the objects.

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
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _rm(self, path: str) -> None:
        bucket, key, _ = self._split_path(path)

        try:
            self.tos_client.delete_object(bucket, key)
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
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
        name = "/".join([bucket, common_prefix.prefix[:-1]])
        return {
            "name": name,
            "Key": name,
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

    @staticmethod
    def _fill_bucket_info(bucket_name: str) -> dict:
        return {
            "Key": bucket_name,
            "Size": 0,
            "StorageClass": "BUCKET",
            "size": 0,
            "type": "directory",
            "name": bucket_name,
        }
