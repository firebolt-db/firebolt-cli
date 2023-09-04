import unittest
from collections import namedtuple
from unittest.mock import MagicMock

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
            input="client_id\nclient_secret\naccount_name\ndatabase_name\n\n\n",
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
    rm_init = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    rm = MagicMock()

    rm.databases = mocker.patch.object(ResourceManager, "databases", create=True)
    rm.engines = mocker.patch.object(ResourceManager, "engines", create=True)
    rm.instance_types = mocker.patch.object(
        ResourceManager, "instance_types", create=True
    )

    database_mock = unittest.mock.MagicMock()
    database_mock.name = "mock_db_name"
    database_mock.data_size_full = 2048
    database_mock.region = "mock_region"

    rm.databases.create.return_value = database_mock
    rm.databases.get.return_value = database_mock

    engine_mock = unittest.mock.MagicMock()
    engine_mock.auto_stop = 100
    engine_mock.region="mock_region"
    rm.engines.create.return_value = engine_mock
    rm.engines.get.return_value = engine_mock

    yield rm, database_mock, engine_mock

    rm_init.assert_called_once()


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
        "client_id": "mock_client_id",
        "client_secret": "mock_client_secret",
        "api_endpoint": "mock_api_endpoint",
        "account_name": "mock_account_name",
    }
