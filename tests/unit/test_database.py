import json
from collections import namedtuple
from typing import Callable, Optional, Sequence
from unittest import mock

import pytest
from click.testing import CliRunner
from firebolt.common.exception import AttachedEngineInUseError, FireboltError
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.database import create, describe, drop, list, update
from firebolt_cli.main import main

Database = namedtuple("Database", "name compute_region_key description")


@pytest.fixture(autouse=True)
def configure_cli_autouse(configure_cli: Callable) -> None:
    configure_cli()


def test_database_create(
    fs: FakeFilesystem, configure_resource_manager: Sequence
) -> None:
    rm, databases_mock, _, _, _ = configure_resource_manager

    result = CliRunner().invoke(
        create, "--name test_database --region us-east-1".split()
    )

    databases_mock.create.assert_called_once_with(
        name="test_database", description="", region="us-east-1"
    )
    assert result.exit_code == 0, "non-zero exit code"


def test_database_create_wrong_name(configure_resource_manager: Sequence) -> None:
    rm, databases_mock, _, _, _ = configure_resource_manager
    databases_mock.create.side_effect = RuntimeError("database already exists")

    result = CliRunner(mix_stderr=False).invoke(
        create, "--name test_database --region us-east-1".split()
    )

    databases_mock.create.assert_called_once_with(
        name="test_database", description="", region=mock.ANY
    )

    assert result.stdout == "", "something unexpected is printed"
    assert result.stderr != "", "the error message is not provided"
    assert result.exit_code != 0, "non-zero exit code"


def test_database_create_json_output(configure_resource_manager: Sequence) -> None:
    rm, databases_mock, database_mock, _, _ = configure_resource_manager

    database_mock.name = "test_database"
    database_mock.description = "test_description"
    database_mock.create_time = "time"

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            database_mock.name,
            "--description",
            database_mock.description,
            "--json",
            "--region",
            "us-west-1",
        ],
    )

    databases_mock.create.assert_called_once_with(
        name="test_database", description="test_description", region=mock.ANY
    )
    assert result.exit_code == 0, "non-zero exit code"

    try:
        json.loads(result.stdout)
    except json.decoder.JSONDecodeError:
        assert False, "not a valid json was in the output"


def databases_list_generic_workflow(
    configure_resource_manager: Sequence,
    additional_parameters: Sequence[str],
    return_databases: Sequence,
    name_contains: Optional[str],
    output_validator: Callable[[str], None],
    short_version=False,
) -> None:
    """
    General workflow with different databases and parameters and
    custom callback function for checking the results
    """
    rm, databases_mock, _, _, _ = configure_resource_manager

    databases_mock.get_many.return_value = return_databases

    cli_runner = CliRunner(mix_stderr=False)
    if short_version:
        result = cli_runner.invoke(main, ["db", "ls"] + additional_parameters)
    else:
        result = cli_runner.invoke(list, additional_parameters)

    databases_mock.get_many.assert_called_once_with(name_contains=name_contains)

    output_validator(result.stdout)
    assert result.stderr == ""
    assert result.exit_code == 0, "non-zero exit code"


@pytest.mark.parametrize("short_version", [False, True])
def test_databases_list_happy_path(
    configure_resource_manager: Sequence,
    short_version: bool,
) -> None:
    """
    Test common workflow with some databases and json output
    """
    databases = [
        Database("db_name1", "eu-east-1", ""),
        Database("db_name2", "eu-west-1", ""),
    ]

    databases_list_generic_workflow(
        configure_resource_manager=configure_resource_manager,
        return_databases=databases,
        additional_parameters=[],
        name_contains=None,
        output_validator=lambda x: len(x) > 0,
        short_version=short_version,
    )


def test_databases_list_happy_path_json(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test common workflow with some databases and json output
    """
    databases = [
        Database("db_name1", "eu-east-1", ""),
        Database("db_name2", "eu-west-1", ""),
    ]

    def json_validator(output: str) -> None:
        try:
            j = json.loads(output)
            assert len(j) == len(databases)
        except json.decoder.JSONDecodeError:
            assert False, "not a valid json was in the output"

    databases_list_generic_workflow(
        configure_resource_manager=configure_resource_manager,
        return_databases=databases,
        additional_parameters=["--json"],
        name_contains=None,
        output_validator=json_validator,
    )


def test_databases_list_happy_path_name_contains(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test common workflow with some databases and tabular output
    """
    databases = [
        Database("db_name1", "eu-east-1", ""),
        Database("db_name2", "eu-west-1", ""),
    ]

    def tabular_validator(output: str) -> None:
        assert "db_name1" in output
        assert "db_name2" in output
        assert len(output) > 0

    databases_list_generic_workflow(
        configure_resource_manager=configure_resource_manager,
        return_databases=databases,
        additional_parameters=["--json"],
        name_contains=None,
        output_validator=tabular_validator,
    )


def test_databases_list_happy_path_no_databases(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test common workflow without databases and json output
    """

    def json_validator(output: str) -> None:
        assert len(json.loads(output)) == 0

    databases_list_generic_workflow(
        configure_resource_manager=configure_resource_manager,
        return_databases=[],
        additional_parameters=["--json", "--name-contains", "advanced_filter"],
        name_contains="advanced_filter",
        output_validator=json_validator,
    )


def test_databases_list_failed(configure_resource_manager: Sequence) -> None:
    """
    Test list databases when get_many fails
    """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager

    databases_mock.get_many.side_effect = FireboltError("internal error")

    result = CliRunner(mix_stderr=False).invoke(list)
    databases_mock.get_many.assert_called_once()

    assert result.stderr != ""
    assert result.exit_code != 0


def database_drop_generic_workflow(
    configure_resource_manager: Sequence,
    additional_parameters: Sequence[str],
    input: Optional[str],
    delete_should_be_called: bool,
) -> None:
    rm, databases_mock, database_mock, _, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        drop,
        ["to_drop_database_name"] + additional_parameters,
        input=input,
    )

    databases_mock.get_by_name.assert_called_once_with(name="to_drop_database_name")
    if delete_should_be_called:
        database_mock.delete.assert_called_once_with()

    assert result.exit_code == 0, "non-zero exit code"


def test_database_drop(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing database without confirmation prompt
    """
    database_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=["--yes"],
        input=None,
        delete_should_be_called=True,
    )


def test_database_drop_prompt_yes(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing database with confirmation prompt
    """
    database_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=[],
        input="yes",
        delete_should_be_called=True,
    )


def test_database_drop_prompt_no(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing database with confirmation prompt, and user rejects
    """
    database_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=[],
        input="no",
        delete_should_be_called=False,
    )


def test_database_drop_not_found(configure_resource_manager: Sequence) -> None:
    """
    Trying to drop the database, if the database is not found by name
    """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    databases_mock.get_by_name.side_effect = RuntimeError("database not found")

    result = CliRunner(mix_stderr=False).invoke(
        drop,
        ["to_drop_database_name"],
    )

    databases_mock.get_by_name.assert_called_once_with(name="to_drop_database_name")

    assert result.stderr != "", "cli should fail with an error message in stderr"
    assert result.exit_code != 0, "non-zero exit code"


def test_database_drop_wrong_state(configure_resource_manager: Sequence) -> None:
    """
    Trying to drop the database, if an attached engine is running
    """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager

    database_mock.delete.side_effect = AttachedEngineInUseError(
        "database has a running engine"
    )

    result = CliRunner(mix_stderr=False).invoke(
        drop, ["to_drop_database_name", "--yes"]
    )

    databases_mock.get_by_name.assert_called_once_with(name="to_drop_database_name")
    database_mock.delete.assert_called_once_with()

    assert result.stderr != "", "cli should fail with an error message in stderr"
    assert result.exit_code != 0, "non-zero exit code"


def test_database_describe_happy_path(configure_resource_manager: Sequence) -> None:
    """ """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    database_mock.data_size_full = 100

    result = CliRunner(mix_stderr=False).invoke(describe, ["to_describe_database"])

    assert result.stderr == ""
    assert result.exit_code == 0


def test_database_describe_json(configure_resource_manager: Sequence) -> None:
    """ """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    database_mock.data_size_full = 2048
    database_mock.name = "to_describe_database"
    database_mock.description = "db description"

    result = CliRunner(mix_stderr=False).invoke(
        describe, ["to_describe_database", "--json"]
    )
    database_description = json.loads(result.stdout)
    assert "name" in database_description
    assert "description" in database_description
    assert "data_size" in database_description
    assert "attached_engine_names" in database_description

    assert database_description["name"] == "to_describe_database"
    assert database_description["description"] == "db description"
    assert database_description["data_size"] == "2 KB"

    assert result.stderr == ""
    assert result.exit_code == 0


def test_database_describe_not_found(configure_resource_manager: Sequence) -> None:
    """ """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    databases_mock.get_by_name.side_effect = FireboltError("db not found")

    result = CliRunner(mix_stderr=False).invoke(describe, ["to_describe_database"])

    assert result.stderr != ""
    assert result.exit_code != 0


def test_database_update_happy_path(configure_resource_manager: Sequence):
    """
    test database update command happy path
    """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    database_mock.name = "db_name"
    database_mock.description = "new description"
    database_mock.update.return_value = database_mock

    result = CliRunner(mix_stderr=False).invoke(
        update, ["--name", "db_name", "--description", "new description", "--json"]
    )
    database_description = json.loads(result.stdout)
    assert database_description["name"] == "db_name"
    assert database_description["description"] == "new description"

    databases_mock.get_by_name.assert_called_once_with(name="db_name")
    database_mock.update.assert_called_once_with(description="new description")

    assert result.stderr == ""
    assert result.exit_code == 0


def test_database_update_not_found(configure_resource_manager: Sequence):
    """
    test database update command, if database doesn't exist
    """
    rm, databases_mock, database_mock, _, _ = configure_resource_manager
    databases_mock.get_by_name.side_effect = FireboltError("db not found")

    result = CliRunner(mix_stderr=False).invoke(
        update, ["--name", "dn_name", "--description", "db_description"]
    )
    assert result.stderr != ""
    assert result.exit_code != 0
