# tosfs

[![PyPI version](https://badge.fury.io/py/tosfs.svg)](https://pypi.python.org/pypi/tosfs/)
[![Status](https://img.shields.io/pypi/status/tosfs.svg)](https://pypi.org/project/tosfs/)
[![Python Version](https://img.shields.io/pypi/pyversions/tosfs.svg)](https://pypi.org/project/tosfs/)
[![License](https://img.shields.io/pypi/l/tosfs)](https://opensource.org/licenses/Apache-2.0)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Doc Status](https://readthedocs.org/projects/tosfs/badge/?version=latest)](https://tosfs.readthedocs.io/en/latest/?badge=latest)
![Build](https://github.com/volcengine/tosfs/workflows/CI/badge.svg)


TOSFS builds on [Volcengine TOS Python SDK](https://github.com/volcengine/ve-tos-python-sdk) to provide a convenient Python filesystem interface for [TOS（Tinder Object Storage）](https://www.volcengine.com/docs/6349/74820).

## Features

* Excellent write performance (optimized by multi-threading and multi-disk staging).
* Solid stability (fine-grained judgment on response codes for TOS services).
* Outstanding compatibility (cross-validation completed on the version matrix of four Python versions and two fsspec versions).
* TOS HNS (Hierarchical NameSpace) Bucket support (in adaptation and verification).

## Installation(TODO)

You can install tosfs via [pip](https://pip.pypa.io/) from [PyPI](https://pypi.org/):

```shell
$ pip install tosfs
```

## Quick Start

### Init FileSystem

* Init via `ak/sk`

```python
from tosfs.core import TosFileSystem

tosfs = TosFileSystem(
    key='ak',
    secret='sk',
    endpoint_url='http://tos-cn-beijing.volces.com',
    region='cn-beijing',
)
```

* Init via system env

make sure these envs take effect:

```shell
export TOS_ACCESS_KEY=' your ak here '
export TOS_SECRET_KEY=' your sk here '
export TOS_ENDPOINT='http://tos-cn-beijing.volces.com'
export TOS_REGION='cn-beijing'
```
then init `TosFileSystem` by setting `credentials_provider` to `EnvCredentialsProvider`

```python
import os
from tosfs.core import TosFileSystem
from tos import EnvCredentialsProvider

tosfs = TosFileSystem(
    endpoint_url=os.environ.get("TOS_ENDPOINT"),
    region=os.environ.get("TOS_REGION"),
    credentials_provider=EnvCredentialsProvider, # must
)
```

### Access FS and file operation APIs

After getting the instance of `TosFileSystem` by following the above guidance,
now we can access fs and file operation apis, e.g.

```python
# list
tosfs.ls("")
tosfs.ls("mybucket", detail=False)
tosfs.ls("mybucket/rootpath/", detail=False)

# file read/write
with tosfs.open('bucket/root/text.txt', mode='wb') as f:
    f.write('hello tosfs!')
    
with tosfs.open('bucket/root/text.txt', mode='rb') as f:
    content = f.read()
    print(content)
```

## Compatibility

The tosfs package is compatible with the following Python and fsspec versions:

* Python

| Version | Supported |
|---------|-----------|
| 3.9     | ✅         |
| 3.10    | ✅         |
| 3.11    | ✅         |
| 3.12    | ✅         |

* fsspec

| Version          | Supported |
|---------------|------|
| 2023.5.0      | ✅   |
| 2024.9.0      | ✅   |

## Contributing
Contributions are very welcome. To learn more, see the [Contributor Guide](https://github.com/volcengine/tosfs/blob/main/CONTRIBUTING.md).

## License
Distributed under the terms of the [Apache 2.0 license](https://github.com/volcengine/tosfs/blob/main/LICENSE), Tosfs is free and open source software.

## Issues
If you encounter any problems, please [file an issue](https://github.com/volcengine/tosfs/issues/new/choose) along with a detailed description.




