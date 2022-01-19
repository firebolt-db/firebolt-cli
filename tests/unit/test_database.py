import json
from collections import namedtuple
from typing import Callable, Optional, Sequence
from unittest import mock

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.configure import configure
from firebolt_cli.database import create, list

Database = namedtuple("Database", "name description")


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


def databases_list_generic_workflow(
    mocker: MockerFixture,
    additional_parameters: Sequence[str],
    return_databases: Sequence,
    name_contains: Optional[str],
    output_validator: Callable[[str], None],
) -> None:
    """
    General workflow with different databases and parameters and
    custom callback function for checking the results
    """
    mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)

    databases_mock.get_many.return_value = return_databases

    result = CliRunner(mix_stderr=False).invoke(list, additional_parameters)
    databases_mock.get_many.assert_called_once_with(name_contains=name_contains)

    output_validator(result.stdout)
    assert result.stderr == ""
    assert result.exit_code == 0


def test_databases_list_happy_path_json(mocker: MockerFixture) -> None:
    """
    Test common workflow with some databases and json output
    """
    databases = [Database("db_name1", ""), Database("db_name2", "")]

    def json_validator(output: str) -> None:
        try:
            j = json.loads(output)
            assert len(j) == len(databases)
        except json.decoder.JSONDecodeError:
            assert False, "not a valid json was in the output"

    databases_list_generic_workflow(
        mocker=mocker,
        return_databases=databases,
        additional_parameters=["--json"],
        name_contains=None,
        output_validator=json_validator,
    )


def test_databases_list_happy_path_name_contains(mocker: MockerFixture) -> None:
    """
    Test common workflow with some databases and tabular output
    """
    databases = [Database("db_name1", ""), Database("db_name2", "")]

    def tabular_validator(output: str) -> None:
        assert "db_name1" in output
        assert "db_name2" in output
        assert len(output) > 0

    databases_list_generic_workflow(
        mocker=mocker,
        return_databases=databases,
        additional_parameters=["--json"],
        name_contains=None,
        output_validator=tabular_validator,
    )


def test_databases_list_happy_path_no_databases(mocker: MockerFixture) -> None:
    """
    Test common workflow without databases and json output
    """

    def json_validator(output: str) -> None:
        assert len(json.loads(output)) == 0

    databases_list_generic_workflow(
        mocker=mocker,
        return_databases=[],
        additional_parameters=["--json", "--name-contains", "advanced_filter"],
        name_contains="advanced_filter",
        output_validator=json_validator,
    )


def test_databases_list_failed(mocker: MockerFixture) -> None:
    """
    Test list databases when get_many fails
    """
    mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)

    databases_mock.get_many.side_effect = FireboltError("internal error")

    result = CliRunner(mix_stderr=False).invoke(list)
    databases_mock.get_many.assert_called_once()

    assert result.stderr != ""
    assert result.exit_code != 0
