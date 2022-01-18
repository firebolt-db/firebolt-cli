import json
from unittest import mock

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from firebolt.service.manager import ResourceManager
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.configure import configure
from firebolt_cli.database import create


@pytest.fixture(autouse=True)
def configure_cli(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    runner.invoke(
        configure,
        [
            "--username",
            "username",
            "--account-name",
            "account_name",
            "--database-name",
            "database_name",
            "--engine-name",
            "engine_name",
            "--api-endpoint",
            "api_endpoint",
        ],
        input="password",
    )


def test_database_create(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)

    result = CliRunner().invoke(
        create,
        [
            "--name",
            "test_database",
        ],
    )

    rm.assert_called()
    databases_mock.create.assert_called_once_with(
        name="test_database", description="", region=mock.ANY
    )
    assert result.exit_code == 0, "non-zero exit code"


def test_database_create_wrong_name(mocker: MockerFixture) -> None:
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)
    databases_mock.create.side_effect = RuntimeError("database already exists")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "test_database",
        ],
    )

    rm.assert_called()
    databases_mock.create.assert_called_once_with(
        name="test_database", description="", region=mock.ANY
    )

    assert result.stdout == "", "something unexpected is printed"
    assert result.stderr != "", "the error message is not provided"
    assert result.exit_code != 0, "non-zero exit code"


def test_database_create_json_output(mocker: MockerFixture) -> None:
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)
    database_mock = mocker.PropertyMock()
    database_mock.name = "test_database"
    database_mock.description = "test_description"
    database_mock.create_time = "time"

    databases_mock.create.return_value = database_mock

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            database_mock.name,
            "--description",
            database_mock.description,
            "--json",
        ],
    )

    rm.assert_called()
    databases_mock.create.assert_called_once_with(
        name="test_database", description="test_description", region=mock.ANY
    )
    assert result.exit_code == 0, "non-zero exit code"

    try:
        json.loads(result.stdout)
    except json.decoder.JSONDecodeError:
        assert False, "not a valid json was in the output"
