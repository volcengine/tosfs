# tosfs

[![PyPI version](https://badge.fury.io/py/tosfs.svg)](https://pypi.python.org/pypi/tosfs/)
![Build](https://github.com/volcengine/tosfs/workflows/CI/badge.svg)

TOSFS builds on [Volcengine TOS Python SDK](https://github.com/volcengine/ve-tos-python-sdk) to provide a convenient Python filesystem interface for [TOS（Tinder Object Storage）](https://www.volcengine.com/docs/6349/74820).

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
from tos import StaticCredentialsProvider

tosfs = TosFileSystem(
    key='ak',
    secret='sk',
    endpoint_url='http://tos-cn-beijing.volces.com',
    region='cn-beijing',
    credentials_provider=StaticCredentialsProvider, # optional
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
