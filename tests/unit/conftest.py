import unittest
from collections import namedtuple

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from firebolt.service.manager import ResourceManager
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.configure import configure


@pytest.fixture()
def configure_cli(fs: FakeFilesystem) -> None:
    def inner_configure_cli():
        fs.create_dir(user_config_dir())
        runner = CliRunner()
        result = runner.invoke(
            configure,
            [],
            input="username\npassword\naccount_name\ndatabase_name\n\n\n",
        )

        assert result.exit_code == 0, "configuration of cli failed"

    return inner_configure_cli


@pytest.fixture()
def cursor_mock(mocker: MockerFixture) -> unittest.mock.Mock:
    connect_function_mock = mocker.patch("firebolt_cli.query.create_connection")

    connection_mock = unittest.mock.MagicMock()
    connect_function_mock.return_value = connection_mock

    cursor_mock = unittest.mock.MagicMock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.__enter__.return_value = connection_mock

    yield cursor_mock

    connection_mock.cursor.assert_called_once_with()
    connect_function_mock.assert_called_once()
    connection_mock.__exit__.assert_called_once()


@pytest.fixture()
def configure_resource_manager(mocker: MockerFixture) -> ResourceManager:
    """
    Configure resource manager mock
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)

    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)
    instance_types_mock = mocker.patch.object(
        ResourceManager, "instance_types", create=True
    )
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)
    mocker.patch.object(ResourceManager, "bindings", create=True)
    regions_mock = mocker.patch.object(ResourceManager, "regions", create=True)
    mocker.patch.object(ResourceManager, "engine_revisions", create=True)

    _Region = namedtuple("Region", "name")
    regions_mock.get_by_key.return_value = _Region("us-east-1")
    regions_mock.regions_by_name = dict({"us-east-1": _Region("us-east-1")})

    _InstanceType = namedtuple(
        "InstanceType",
        "name, cpu_virtual_cores_count, memory_size_bytes, storage_size_bytes",
    )
    instance_types_mock.get_instance_types_per_region.return_value = [
        _InstanceType("B1", 2, 123, 321)
    ]

    database_mock = unittest.mock.MagicMock()
    database_mock.name = "mock_db_name"
    database_mock.data_size_full = 2048

    databases_mock.create.return_value = database_mock
    databases_mock.get_by_name.return_value = database_mock

    engine_mock = unittest.mock.MagicMock()
    engines_mock.create.return_value = engine_mock
    engines_mock.get_by_name.return_value = engine_mock

    yield rm, databases_mock, database_mock, engines_mock, engine_mock

    rm.assert_called_once()


@pytest.fixture()
def mock_table_config() -> dict:
    return {
        "table_name": "test_table",
        "columns": [
            {"name": "test_col_1", "type": "INT"},
            {"name": "test_col_2", "type": "TEXT"},
        ],
        "file_type": "PARQUET",
        "object_pattern": ["*.parquet"],
        "primary_index": ["test_col_1"],
    }


@pytest.fixture()
def mock_connection_params() -> dict:
    return {
        "engine_name": "mock_engine_name",
        "database_name": "mock_database_name",
        "username": "mock_username",
        "password": "mock_password",
        "access_token": "mock_access_token",
        "api_endpoint": "mock_api_endpoint",
        "account_name": "mock_account_name",
    }
