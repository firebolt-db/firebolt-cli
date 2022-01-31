import unittest

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
            [
                "--username",
                "username",
                "--account-name",
                "account_name",
                "--engine-name",
                "engine_name",
                "--api-endpoint",
                "api_endpoint",
                "--database-name",
                "default",
            ],
            input="password",
        )

        assert result.exit_code == 0, "configuration of cli failed"

    return inner_configure_cli


@pytest.fixture()
def cursor_mock(mocker: MockerFixture) -> unittest.mock.Mock:
    connect_function_mock = mocker.patch("firebolt_cli.query.connect")

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
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)

    database_mock = unittest.mock.MagicMock()
    databases_mock.get_by_name.return_value = database_mock

    engine_mock = unittest.mock.MagicMock()
    engines_mock.create.return_value = engine_mock

    yield rm, databases_mock, database_mock, engines_mock, engine_mock

    rm.assert_called_once()
