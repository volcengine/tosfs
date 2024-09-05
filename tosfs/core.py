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
import io
import logging
import mimetypes
import os
import time
from typing import Any, BinaryIO, Generator, List, Optional, Tuple, Union

import tos
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import setup_logging as setup_logger
from tos.models import CommonPrefixInfo
from tos.models2 import (
    CreateMultipartUploadOutput,
    ListedObject,
    ListedObjectVersion,
    PartInfo,
    UploadPartOutput,
)

from tosfs.consts import (
    MANAGED_COPY_THRESHOLD,
    PART_MAX_SIZE,
    RETRY_NUM,
    TOS_SERVER_RESPONSE_CODE_NOT_FOUND,
)
from tosfs.exceptions import TosfsError
from tosfs.utils import find_bucket_key, get_brange, retryable_func_wrapper

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

    protocol = ("tos", "tosfs")
    default_block_size = 5 * 2**20

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        key: str = "",
        secret: str = "",
        region: Optional[str] = None,
        version_aware: bool = False,
        credentials_provider: Optional[object] = None,
        default_block_size: Optional[int] = None,
        default_fill_cache: bool = True,
        default_cache_type: str = "readahead",
        **kwargs: Any,
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
        self.default_block_size = default_block_size or self.default_block_size
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type

        super().__init__(**kwargs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        version_id: Optional[str] = None,
        fill_cache: Optional[bool] = None,
        cache_type: Optional[str] = None,
        autocommit: bool = True,
        **kwargs: Any,
    ) -> AbstractBufferedFile:
        """Open a file for reading or writing.

        Parameters
        ----------
        path: string
            Path of file on TOS
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        block_size: int
            Size of data-node blocks if reading
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        version_id : str
            Explicit version of the object to open.  This requires that the tos
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
        cache_type : str
            See fsspec's documentation for available cache_type values. Set to "none"
            if no caching is desired. If None, defaults to ``self.default_cache_type``.
        autocommit : bool
            If True, writes will be committed to the filesystem on flush or close.
        kwargs: dict-like
            Additional parameters.

        """
        if block_size is None:
            block_size = self.default_block_size
        if fill_cache is None:
            fill_cache = self.default_fill_cache

        if not self.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )

        if cache_type is None:
            cache_type = self.default_cache_type

        return TosFile(
            self,
            path,
            mode,
            block_size=block_size,
            version_id=version_id,
            fill_cache=fill_cache,
            cache_type=cache_type,
            autocommit=autocommit,
        )

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

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory entry at path.

        For systems that don't have true directories, may create an object for
        this instance only and not touch the real filesystem

        Parameters
        ----------
        path: str
            location
        create_parents: bool
            if True, this is equivalent to ``makedirs``
        kwargs: Any
            may be permissions, etc.

        """
        path = self._strip_protocol(path).rstrip("/") + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            raise TosfsError(f"Cannot create a bucket {bucket} using mkdir api.")

        try:
            if create_parents:
                parent = self._parent(f"{bucket}/{key}".rstrip("/") + "/")
                if not self.exists(parent):
                    # here we need to create the parent directory recursively
                    self.mkdir(parent, create_parents=True)
                self.tos_client.put_object(bucket, key.rstrip("/") + "/")
            else:
                parent = self._parent(path)
                if not self.exists(parent):
                    raise FileNotFoundError(
                        f"Parent directory {parent} does not exist."
                    )
                else:
                    self.tos_client.put_object(bucket, key.rstrip("/") + "/")
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Recursively make directories.

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If False, will error if the target already exists

        """
        path = self._strip_protocol(path).rstrip("/") + "/"
        if exist_ok and self.exists(path):
            return
        if not exist_ok and self.exists(path):
            raise FileExistsError(path)

        self.mkdir(path, create_parents=True)

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

    def put_file(
        self,
        lpath: str,
        rpath: str,
        chunksize: int = 5 * 2**20,
        **kwargs: Any,
    ) -> None:
        """Put a file from local to TOS.

        Parameters
        ----------
        lpath : str
            The local path of the file to put.
        rpath : str
            The remote path of the file to put.
        chunksize : int, optional
            The size of the chunks to read from the file (default is 5 * 2**20).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the local file does not exist.
        IsADirectoryError
            If the local path is a directory.
        TosClientError
            If there is a client error while putting the file.
        TosServerError
            If there is a server error while putting the file.
        TosfsError
            If there is an unknown error while putting the file.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.put_file("localfile.txt", "tos://mybucket/remote.txt")

        """
        if not os.path.exists(lpath):
            raise FileNotFoundError(f"Local file {lpath} not found.")

        if os.path.isdir(lpath):
            raise IsADirectoryError(f"{lpath} is a directory.")

        size = os.path.getsize(lpath)

        content_type = None
        if "ContentType" not in kwargs:
            content_type, _ = mimetypes.guess_type(lpath)

        bucket, key, _ = self._split_path(rpath)

        try:
            if self.isfile(rpath):
                self.makedirs(self._parent(rpath), exist_ok=True)

            if self.isdir(rpath):
                rpath = os.path.join(rpath, os.path.basename(lpath))

            bucket, key, _ = self._split_path(rpath)

            with open(lpath, "rb") as f:
                if size < min(5 * 2**30, 2 * chunksize):
                    chunk = f.read()
                    self.tos_client.put_object(
                        bucket,
                        key,
                        content=chunk,
                        content_type=content_type,
                    )
                else:
                    mpu = self.tos_client.create_multipart_upload(
                        bucket, key, content_type=content_type
                    )
                    self.tos_client.upload_part_from_file(
                        bucket, key, mpu.upload_id, file_path=lpath, part_number=1
                    )
                    self.tos_client.complete_multipart_upload(
                        bucket, key, mpu.upload_id, complete_all=True
                    )
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """Get a file from the TOS filesystem and write to a local path.

           This method will retry the download if there is error.

        Parameters
        ----------
        rpath : str
            The remote path of the file to get.
        lpath : str
            The local path to save the file.
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        tos.exceptions.TosClientError
            If there is a client error while getting the file.
        tos.exceptions.TosServerError
            If there is a server error while getting the file.
        TosfsError
            If there is an unknown error while getting the file.

        """
        if os.path.isdir(lpath):
            return

        if not self.exists(rpath):
            raise FileNotFoundError(rpath)

        bucket, key, version_id = self._split_path(rpath)

        def _read_chunks(body: BinaryIO, f: BinaryIO) -> None:
            failed_reads = 0
            bytes_read = 0
            while True:
                try:
                    chunk = body.read(2**16)
                except tos.exceptions.TosClientError as e:
                    failed_reads += 1
                    if failed_reads >= self.RETRY_NUM:
                        raise e
                    try:
                        body.close()
                    except Exception as e:
                        logger.error(
                            "Failed to close the body when calling "
                            "get_file from %s to %s : %s",
                            rpath,
                            lpath,
                            e,
                        )

                    time.sleep(min(1.7**failed_reads * 0.1, 15))
                    body, _ = self._open_remote_file(
                        bucket, key, version_id, bytes_read, **kwargs
                    )
                    continue
                if not chunk:
                    break
                bytes_read += len(chunk)
                f.write(chunk)

        body, content_length = self._open_remote_file(
            bucket, key, version_id, range_start=0, **kwargs
        )
        try:
            with open(lpath, "wb") as f:
                _read_chunks(body, f)
        finally:
            try:
                body.close()
            except Exception as e:
                logger.error(
                    "Failed to close the body when calling "
                    "get_file from %s to %s: %s",
                    rpath,
                    lpath,
                    e,
                )

    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        on_error: str = "omit",
        **kwargs: Any,
    ) -> Generator[str, List[str], List[str]]:
        """List objects under the given path.

        Parameters
        ----------
        path : str
            The path to list.
        maxdepth : int, optional
            The maximum depth to walk to (default is None).
        topdown : bool, optional
            Whether to walk top-down or bottom-up (default is True).
        on_error : str, optional
            How to handle errors (default is 'omit').
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        ValueError
            If the path is an invalid path.

        """
        if path in ["", "*"] + ["{}://".format(p) for p in self.protocol]:
            raise ValueError("Cannot access all of TOS via path {}.".format(path))

        return super().walk(
            path, maxdepth=maxdepth, topdown=topdown, on_error=on_error, **kwargs
        )

    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        prefix: str = "",
        **kwargs: Any,
    ) -> Union[List[str], dict]:
        """Find all files or dirs with conditions.

        Like posix ``find`` command without conditions

        Parameters
        ----------
        path : str
            The path to search.
        maxdepth: int, optional
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        prefix: str
            Only return files that match ``^{path}/{prefix}`` (if there is an
            exact match ``filename == {path}/{prefix}``, it also will be included)
        detail: bool
            If True, return a dict with file information, else just the path
        **kwargs: Any
            Additional arguments.

        """
        if path in ["", "*"] + ["{}://".format(p) for p in self.protocol]:
            raise ValueError("Cannot access all of TOS via path {}.".format(path))

        path = self._strip_protocol(path)
        bucket, key, _ = self._split_path(path)
        if not bucket:
            raise ValueError("Cannot access all of TOS without specify a bucket.")

        if maxdepth and prefix:
            raise ValueError(
                "Can not specify 'prefix' option alongside 'maxdepth' options."
            )
        if maxdepth:
            return super().find(
                bucket + "/" + key,
                maxdepth=maxdepth,
                withdirs=withdirs,
                detail=detail,
                **kwargs,
            )

        out = self._find_file_dir(key, path, prefix, withdirs, kwargs)

        if detail:
            return {o["name"]: o for o in out}
        else:
            return [o["name"] for o in out]

    def cp_file(
        self,
        path1: str,
        path2: str,
        preserve_etag: Optional[bool] = None,
        managed_copy_threshold: Optional[int] = MANAGED_COPY_THRESHOLD,
        **kwargs: Any,
    ) -> None:
        """Copy file between locations on tos.

        Parameters
        ----------
        path1 : str
            The source path of the file to copy.
        path2 : str
            The destination path of the file to copy.
        preserve_etag : bool, optional
            Whether to preserve etag while copying. If the file is uploaded
            as a single part, then it will be always equivalent to the md5
            hash of the file hence etag will always be preserved. But if the
            file is uploaded in multi parts, then this option will try to
            reproduce the same multipart upload while copying and preserve
            the generated etag.
        managed_copy_threshold : int, optional
            The threshold size of the file to copy using managed copy. If the
            size of the file is greater than this threshold, then the file
            will be copied using managed copy (default is 5 * 2**30).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the source file does not exist.
        ValueError
            If the destination is a versioned file.
        TosClientError
            If there is a client error while copying the file.
        TosServerError
            If there is a server error while copying the file.
        TosfsError
            If there is an unknown error while copying the file.

        """
        path1 = self._strip_protocol(path1)
        bucket, key, vers = self._split_path(path1)

        info = self.info(path1, bucket, key, version_id=vers)
        size = info["size"]

        _, _, parts_suffix = info.get("ETag", "").strip('"').partition("-")
        if preserve_etag and parts_suffix:
            self._copy_etag_preserved(path1, path2, size, total_parts=int(parts_suffix))
        elif size <= min(
            MANAGED_COPY_THRESHOLD,
            (
                managed_copy_threshold
                if managed_copy_threshold
                else MANAGED_COPY_THRESHOLD
            ),
        ):
            self._copy_basic(path1, path2, **kwargs)
        else:
            # if the preserve_etag is true, either the file is uploaded
            # on multiple parts or the size is lower than 5GB
            assert not preserve_etag

            # serial multipart copy
            self._copy_managed(path1, path2, size, **kwargs)

    def _copy_basic(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy file between locations on tos.

        Not allowed where the origin is larger than 5GB.
        """
        buc1, key1, ver1 = self._split_path(path1)
        buc2, key2, ver2 = self._split_path(path2)
        if ver2:
            raise ValueError("Cannot copy to a versioned file!")
        try:
            self.tos_client.copy_object(
                bucket=buc2,
                key=key2,
                src_bucket=buc1,
                src_key=key1,
                src_version_id=ver1,
            )
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
            raise e
        except Exception as e:
            raise TosfsError("Copy failed (%r -> %r): %s" % (path1, path2, e)) from e

    def _copy_etag_preserved(
        self, path1: str, path2: str, size: int, total_parts: int, **kwargs: Any
    ) -> None:
        """Copy file as multiple-part while preserving the etag."""
        bucket1, key1, version1 = self._split_path(path1)
        bucket2, key2, version2 = self._split_path(path2)

        upload_id = None

        try:
            mpu = self.tos_client.create_multipart_upload(bucket2, key2)
            upload_id = mpu.upload_id

            parts = []
            brange_first = 0

            for i in range(1, total_parts + 1):
                part_size = min(size - brange_first, PART_MAX_SIZE)
                brange_last = brange_first + part_size - 1
                if brange_last > size:
                    brange_last = size - 1

                part = self.tos_client.upload_part_copy(
                    bucket=bucket2,
                    key=key2,
                    part_number=i,
                    upload_id=upload_id,
                    src_bucket=bucket1,
                    src_key=key1,
                    copy_source_range_start=brange_first,
                    copy_source_range_end=brange_last,
                )
                parts.append(
                    PartInfo(
                        part_number=part.part_number,
                        etag=part.etag,
                        part_size=size,
                        offset=None,
                        hash_crc64_ecma=None,
                        is_completed=None,
                    )
                )
                brange_first += part_size

            self.tos_client.complete_multipart_upload(bucket2, key2, upload_id, parts)
        except Exception as e:
            self.tos_client.abort_multipart_upload(bucket2, key2, upload_id)
            raise TosfsError(f"Copy failed ({path1} -> {path2}): {e}") from e

    def _copy_managed(
        self,
        path1: str,
        path2: str,
        size: int,
        block: int = MANAGED_COPY_THRESHOLD,
        **kwargs: Any,
    ) -> None:
        """Copy file between locations on tos as multiple-part.

        block: int
            The size of the pieces, must be larger than 5MB and at
            most MANAGED_COPY_THRESHOLD.
            Smaller blocks mean more calls, only useful for testing.
        """
        if block < 5 * 2**20 or block > MANAGED_COPY_THRESHOLD:
            raise ValueError("Copy block size must be 5MB<=block<=5GB")

        bucket1, key1, version1 = self._split_path(path1)
        bucket2, key2, version2 = self._split_path(path2)

        upload_id = None

        try:
            mpu = self.tos_client.create_multipart_upload(bucket2, key2)
            upload_id = mpu.upload_id
            out = [
                self.tos_client.upload_part_copy(
                    bucket=bucket2,
                    key=key2,
                    part_number=i + 1,
                    upload_id=upload_id,
                    src_bucket=bucket1,
                    src_key=key1,
                    copy_source_range_start=brange_first,
                    copy_source_range_end=brange_last,
                )
                for i, (brange_first, brange_last) in enumerate(get_brange(size, block))
            ]

            parts = [
                PartInfo(
                    part_number=i + 1,
                    etag=o.etag,
                    part_size=size,
                    offset=None,
                    hash_crc64_ecma=None,
                    is_completed=None,
                )
                for i, o in enumerate(out)
            ]

            self.tos_client.complete_multipart_upload(bucket2, key2, upload_id, parts)
        except Exception as e:
            self.tos_client.abort_multipart_upload(bucket2, key2, upload_id)
            raise TosfsError(f"Copy failed ({path1} -> {path2}): {e}") from e

    def _find_file_dir(
        self, key: str, path: str, prefix: str, withdirs: bool, kwargs: Any
    ) -> List[dict]:
        out = self._lsdir(
            path, delimiter="", include_self=True, prefix=prefix, **kwargs
        )
        if not out and key:
            try:
                out = [self.info(path)]
            except FileNotFoundError:
                out = []
        dirs = []
        for o in out:
            par = self._parent(o["name"])
            if len(path) <= len(par):
                d = {
                    "Key": self._split_path(par)[1],
                    "Size": 0,
                    "name": par,
                    "type": "directory",
                }
                dirs.append(d)
        if withdirs:
            out = sorted(out + dirs, key=lambda x: x["name"])
        else:
            out = [o for o in out if o["type"] == "file"]
        return out

    def _open_remote_file(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str],
        range_start: int,
        **kwargs: Any,
    ) -> Tuple[BinaryIO, int]:
        try:
            resp = self.tos_client.get_object(
                bucket,
                key,
                version_id=version_id,
                range_start=range_start,
                **kwargs,
            )
            return resp.content, resp.content_length
        except tos.exceptions.TosClientError as e:
            raise e
        except tos.exceptions.TosServerError as e:
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
        include_self: bool = False,
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
        include_self : bool, optional
            Whether to include the directory itself in the listing (default is False).
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
            include_self=include_self,
            versions=versions,
        ):
            if isinstance(obj, CommonPrefixInfo):
                dirs.append(self._fill_dir_info(bucket, obj))
            elif obj.key.endswith("/"):
                dirs.append(self._fill_dir_info(bucket, None, obj.key))
            else:
                files.append(self._fill_file_info(obj, bucket, versions))
        files += dirs

        return files

    def _listdir(
        self,
        bucket: str,
        max_items: int = 1000,
        delimiter: str = "/",
        prefix: str = "",
        include_self: bool = False,
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
        include_self : bool, optional
            Whether to include the bucket itself in the listing (default is False).
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
                        start_after=prefix if not include_self else None,
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

        if path.endswith("/") or self.isdir(path):
            key = key.rstrip("/") + "/"

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
        >>> self._split_path("tos://mybucket/path/to/file")
        ['mybucket', 'path/to/file', None]
        # pylint: disable=line-too-long
        >>> self._split_path("tos://mybucket/path/to/versioned_file?versionId=some_version_id")
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
    def _fill_dir_info(
        bucket: str, common_prefix: Optional[CommonPrefixInfo], key: str = ""
    ) -> dict:
        name = "/".join([bucket, common_prefix.prefix[:-1] if common_prefix else key])
        return {
            "name": name,
            "Key": name,
            "Size": 0,
            "type": "directory",
        }

    @staticmethod
    def _fill_file_info(obj: ListedObject, bucket: str, versions: bool = False) -> dict:
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


class TosFile(AbstractBufferedFile):
    """File-like operations for TOS."""

    def __init__(
        self,
        fs: TosFileSystem,
        path: str,
        mode: str = "rb",
        block_size: Union[int, str] = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        **kwargs: Any,
    ):
        """Instantiate a TOS file."""
        bucket, key, path_version_id = fs._split_path(path)
        if not key:
            raise ValueError("Attempt to open non key-like path: %s" % path)
        super().__init__(
            fs,
            path,
            mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            **kwargs,
        )
        self.fs = fs
        self.bucket = bucket
        self.key = key
        self.path = path
        self.mode = mode
        self.autocommit = autocommit
        self.mpu: CreateMultipartUploadOutput = None
        self.parts: Optional[list] = None
        self.append_block = False
        self.buffer: Optional[io.BytesIO] = io.BytesIO()

    def _initiate_upload(self) -> None:
        """Create remote file/upload."""
        if self.autocommit and not self.append_block and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        logger.debug("Initiate upload for %s", self)
        self.parts = []

        self.mpu = self.fs.tos_client.create_multipart_upload(self.bucket, self.key)

        if self.append_block:
            # use existing data in key when appending,
            # and block is big enough
            out = self.fs.tos_client.upload_part_copy(
                bucket=self.bucket,
                key=self.key,
                part_number=1,
                upload_id=self.mpu.upload_id,
            )

            self.parts.append({"PartNumber": out.part_number, "ETag": out.etag})

    def _upload_chunk(self, final: bool = False) -> bool:
        """Write one part of a multi-block file upload.

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.

        """
        bucket, key, _ = self.fs._split_path(self.path)
        if self.buffer:
            logger.debug(
                "Upload for %s, final=%s, loc=%s, buffer loc=%s",
                self,
                final,
                self.loc,
                self.buffer.tell(),
            )

        if (
            self.autocommit
            and not self.append_block
            and final
            and self.tell() < self.blocksize
        ):
            # only happens when closing small file, use one-shot PUT
            pass
        else:
            self._upload_multiple_chunks(bucket, key)

        if self.autocommit and final:
            self.commit()

        return not final

    def _upload_multiple_chunks(self, bucket: str, key: str) -> None:
        if self.buffer:
            self.buffer.seek(0)
            current_chunk: Optional[bytes] = self.buffer.read(self.blocksize)

            while current_chunk:
                (previous_chunk, current_chunk) = (
                    current_chunk,
                    self.buffer.read(self.blocksize) if self.buffer else None,
                )
                current_chunk_size = len(current_chunk if current_chunk else b"")

                # Define a helper function to handle the remainder logic
                def handle_remainder(
                    previous_chunk: bytes,
                    current_chunk: Optional[bytes],
                    blocksize: int,
                    part_max: int,
                ) -> Tuple[bytes, Optional[bytes]]:
                    if current_chunk:
                        remainder = previous_chunk + current_chunk
                    else:
                        remainder = previous_chunk

                    remainder_size = (
                        blocksize + len(current_chunk) if current_chunk else blocksize
                    )

                    if remainder_size <= part_max:
                        return remainder, None
                    else:
                        partition = remainder_size // 2
                        return remainder[:partition], remainder[partition:]

                # Use the helper function in the main code
                if 0 < current_chunk_size < self.blocksize:
                    previous_chunk, current_chunk = handle_remainder(
                        previous_chunk, current_chunk, self.blocksize, PART_MAX_SIZE
                    )

                part = len(self.parts) + 1 if self.parts is not None else 1
                logger.debug("Upload chunk %s, %s", self, part)

                out: UploadPartOutput = self.fs.tos_client.upload_part(
                    bucket=bucket,
                    key=key,
                    part_number=part,
                    upload_id=self.mpu.upload_id,
                    content=previous_chunk,
                )

                (
                    self.parts.append(
                        PartInfo(
                            part_number=part,
                            etag=out.etag,
                            part_size=len(previous_chunk),
                            offset=None,
                            hash_crc64_ecma=None,
                            is_completed=None,
                        )
                    )
                    if self.parts is not None
                    else None
                )

    def _fetch_range(self, start: int, end: int) -> bytes:
        bucket, key, version_id = self.fs._split_path(self.path)
        if start == end:
            logger.debug(
                "skip fetch for negative range - bucket=%s,key=%s,start=%d,end=%d",
                bucket,
                key,
                start,
                end,
            )
            return b""
        logger.debug("Fetch: %s/%s, %s-%s", bucket, key, start, end)

        def fetch() -> bytes:
            return self.fs.tos_client.get_object(
                bucket, key, version_id, range_start=start, range_end=end
            ).read()

        return retryable_func_wrapper(fetch, retries=RETRY_NUM)

    def commit(self) -> None:
        """Complete multipart upload or PUT."""
        logger.debug("Commit %s", self)
        if self.tell() == 0:
            if self.buffer is not None:
                logger.debug("Empty file committed %s", self)
                self._abort_mpu()
                self.fs.touch(self.path, **self.kwargs)
        elif not self.parts:
            if self.buffer is not None:
                logger.debug("One-shot upload of %s", self)
                self.buffer.seek(0)
                data = self.buffer.read()
                write_result = self.fs.tos_client.put_object(
                    self.bucket, self.key, content=data
                )
            else:
                raise RuntimeError
        else:
            logger.debug("Complete multi-part upload for %s ", self)
            write_result = self.fs.tos_client.complete_multipart_upload(
                self.bucket, self.key, upload_id=self.mpu.upload_id, parts=self.parts
            )

        if self.fs.version_aware:
            self.version_id = write_result.version_id

        self.buffer = None

    def discard(self) -> None:
        """Close the file without writing."""
        self._abort_mpu()
        self.buffer = None  # file becomes unusable

    def _abort_mpu(self) -> None:
        if self.mpu:
            self.fs.tos_client.abort_multipart_upload(
                self.bucket, self.key, self.mpu.upload_id
            )
            self.mpu = None
