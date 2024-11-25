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

from datetime import datetime, timedelta
from unittest.mock import mock_open, patch

import pytest

from tosfs.certification import FileCredentialsProvider
from tosfs.exceptions import TosfsCertificationError


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
<configuration>
    <property>
        <name>fs.tos.access-key-id</name>
        <value>access_key</value>
    </property>
    <property>
        <name>fs.tos.secret-access-key</name>
        <value>secret_key</value>
    </property>
    <property>
        <name>fs.tos.session-token</name>
        <value>session_token</value>
    </property>
</configuration>
""",
)
def test_get_credentials(mock_file):
    provider = FileCredentialsProvider("dummy_path")
    credentials = provider.get_credentials()
    assert credentials.access_key_id == "access_key"
    assert credentials.access_key_secret == "secret_key"  # noqa S105
    assert credentials.security_token == "session_token"  # noqa S105


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
<configuration>
    <property>
        <name>fs.tos.access-key-id</name>
        <value>access_key</value>
    </property>
    <property>
        <name>fs.tos.secret-access-key</name>
        <value>secret_key</value>
    </property>
    <property>
        <name>fs.tos.session-token</name>
        <value>session_token</value>
    </property>
</configuration>
""",
)
def test_get_credentials_with_refresh(mock_file):
    provider = FileCredentialsProvider("dummy_path")
    provider.prev_refresh_time = datetime.now() - timedelta(minutes=61)
    credentials = provider.get_credentials()
    assert credentials.access_key_id == "access_key"
    assert credentials.access_key_secret == "secret_key"  # noqa S105
    assert credentials.security_token == "session_token"  # noqa S105


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
<configuration>
    <property>
        <name>fs.tos.access-key-id</name>
        <value>access_key</value>
    </property>
    <property>
        <name>fs.tos.secret-access-key</name>
        <value>secret_key</value>
    </property>
    <property>
        <name>fs.tos.session-token</name>
        <value>session_token</value>
    </property>
</configuration>
""",
)
def test_get_credentials_error(mock_file):
    provider = FileCredentialsProvider("dummy_path")
    with patch("xml.etree.ElementTree.parse", side_effect=Exception("Parse error")):
        with pytest.raises(TosfsCertificationError):
            provider.get_credentials()


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
<configuration>
    <property>
        <key>fs.tos.access-key-id</name>
        <value>access_key</value>
    </property>
    <property>
        <key>fs.tos.secret-access-key</name>
        <value>secret_key</value>
    </property>
    <property>
        <key>fs.tos.session-token</name>
        <value>session_token</value>
    </property>
</configuration>
""",
)
def test_wrong_file_format_error(mock_file):
    provider = FileCredentialsProvider("dummy_path")
    provider.prev_refresh_time = datetime.now() - timedelta(minutes=61)
    with pytest.raises(TosfsCertificationError):
        provider.get_credentials()


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
<configuration>
    <property>
        <name>fs.tos.access-key-id</name>
        <value>access_key</value>
    </property>
    <property>
        <name>fs.tos.secret-access-key</name>
        <value>secret_key</value>
    </property>
    <property>
        <name>fs.tos.session-token</name>
        <value>session_token</value>
    </property>
</configuration>
""",
)
def test_no_refresh_within_interval(mock_file):
    provider = FileCredentialsProvider("dummy_path")
    provider.get_credentials()

    provider.prev_refresh_time = datetime.now() - timedelta(minutes=30)

    mock_file().read_data = """
    <configuration>
        <property>
            <name>fs.tos.access-key-id</name>
            <value>new_access_key</value>
        </property>
        <property>
            <name>fs.tos.secret-access-key</name>
            <value>new_secret_key</value>
        </property>
        <property>
            <name>fs.tos.session-token</name>
            <value>new_session_token</value>
        </property>
    </configuration>
    """

    new_credentials = provider.get_credentials()

    assert new_credentials.access_key_id == "access_key"
    assert new_credentials.access_key_secret == "secret_key"  # noqa S105
    assert new_credentials.security_token == "session_token"  # noqa S105
