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

"""The module contains the MultipartUploader class for the tosfs package."""

import io
import itertools
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional

from tos.models2 import CreateMultipartUploadOutput, PartInfo

from tosfs.retry import retryable_func_executor

if TYPE_CHECKING:
    from tosfs.core import TosFileSystem


class StagingPartMgr:
    """A class to handle staging parts for multipart upload."""

    def __init__(self, part_size: int, staging_dirs: itertools.cycle):
        """Instantiate a StagingPart object."""
        self.part_size = part_size
        self.staging_dirs = staging_dirs
        self.staging_buffer = io.BytesIO()
        self.staging_files: list[str] = []

    def write_to_buffer(self, chunk: bytes) -> None:
        """Write data to the staging buffer."""
        self.staging_buffer.write(chunk)
        if self.staging_buffer.tell() >= self.part_size:
            self.flush_buffer(False)

    def flush_buffer(self, final: bool = False) -> None:
        """Flush the staging buffer."""
        if self.staging_buffer.tell() == 0:
            return

        buffer_size = self.staging_buffer.tell()
        self.staging_buffer.seek(0)

        while buffer_size >= self.part_size:
            staging_dir = next(self.staging_dirs)
            with tempfile.NamedTemporaryFile(delete=False, dir=staging_dir) as tmp:
                tmp.write(self.staging_buffer.read(self.part_size))
                self.staging_files.append(tmp.name)
                buffer_size -= self.part_size

        if not final:
            remaining_data = self.staging_buffer.read()
            self.staging_buffer = io.BytesIO()
            self.staging_buffer.write(remaining_data)
        else:
            staging_dir = next(self.staging_dirs)
            with tempfile.NamedTemporaryFile(delete=False, dir=staging_dir) as tmp:
                tmp.write(self.staging_buffer.read())
                self.staging_files.append(tmp.name)
            self.staging_buffer = io.BytesIO()

    def get_staging_files(self) -> list[str]:
        """Get the staging files."""
        return self.staging_files

    def clear_staging_files(self) -> None:
        """Clear the staging files."""
        self.staging_files = []


class MultipartUploader:
    """A class to upload large files to the object store using multipart upload."""

    def __init__(
        self,
        fs: "TosFileSystem",
        bucket: str,
        key: str,
        part_size: int,
        thread_pool_size: int,
        multipart_threshold: int,
    ):
        """Instantiate a MultipartUploader object."""
        self.fs = fs
        self.bucket = bucket
        self.key = key
        self.part_size = part_size
        self.thread_pool_size = thread_pool_size
        self.multipart_threshold = multipart_threshold
        self.executor = ThreadPoolExecutor(max_workers=self.thread_pool_size)
        self.staging_part_mgr = StagingPartMgr(
            part_size, itertools.cycle(fs.multipart_staging_dirs)
        )
        self.parts: list = []
        self.mpu: CreateMultipartUploadOutput = None

    def initiate_upload(self) -> None:
        """Initiate the multipart upload."""
        self.mpu = retryable_func_executor(
            lambda: self.fs.tos_client.create_multipart_upload(self.bucket, self.key),
            max_retry_num=self.fs.max_retry_num,
        )

    def upload_multiple_chunks(self, buffer: Optional[io.BytesIO]) -> None:
        """Upload multiple chunks of data to the object store."""
        if buffer:
            buffer.seek(0)
            while True:
                chunk = buffer.read(self.part_size)
                if not chunk:
                    break
                self.staging_part_mgr.write_to_buffer(chunk)

    def upload_staged_files(self) -> None:
        """Upload the staged files to the object store."""
        self.staging_part_mgr.flush_buffer(True)
        futures = []
        for i, staging_file in enumerate(self.staging_part_mgr.get_staging_files()):
            part_number = i + 1
            futures.append(
                self.executor.submit(
                    self._upload_part_from_file, staging_file, part_number
                )
            )

        for future in futures:
            part_info = future.result()
            self.parts.append(part_info)

        self.staging_part_mgr.clear_staging_files()

    def _upload_part_from_file(self, staging_file: str, part_number: int) -> PartInfo:
        with open(staging_file, "rb") as f:
            content = f.read()

        out = retryable_func_executor(
            lambda: self.fs.tos_client.upload_part(
                bucket=self.bucket,
                key=self.key,
                part_number=part_number,
                upload_id=self.mpu.upload_id,
                content=content,
            ),
            max_retry_num=self.fs.max_retry_num,
        )

        os.remove(staging_file)
        return PartInfo(
            part_number=part_number,
            etag=out.etag,
            part_size=len(content),
            offset=None,
            hash_crc64_ecma=None,
            is_completed=None,
        )

    def complete_upload(self) -> None:
        """Complete the multipart upload."""
        retryable_func_executor(
            lambda: self.fs.tos_client.complete_multipart_upload(
                self.bucket,
                self.key,
                upload_id=self.mpu.upload_id,
                parts=self.parts,
            ),
            max_retry_num=self.fs.max_retry_num,
        )

    def abort_upload(self) -> None:
        """Abort the multipart upload."""
        if self.mpu:
            retryable_func_executor(
                lambda: self.fs.tos_client.abort_multipart_upload(
                    self.bucket, self.key, self.mpu.upload_id
                ),
                max_retry_num=self.fs.max_retry_num,
            )
            self.mpu = None
