import json
from collections import namedtuple
from typing import Callable, Optional, Sequence
from unittest import mock

import pytest
from click.testing import CliRunner
from firebolt.common.exception import AttachedEngineInUseError, FireboltError
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.database import create, drop, list

Database = namedtuple("Database", "name description")


@pytest.fixture(autouse=True)
def configure_cli_autouse(configure_cli: Callable) -> None:
    configure_cli()


def test_database_create(
    fs: FakeFilesystem, configure_resource_manager: Sequence
) -> None:
    rm, databases_mock, _, _, _ = configure_resource_manager

    result = CliRunner().invoke(
        create,
        [
            "--name",
            "test_database",
        ],
    )

    databases_mock.create.assert_called_once_with(
        name="test_database", description="", region=mock.ANY
    )
    assert result.exit_code == 0, "non-zero exit code"


def test_database_create_wrong_name(configure_resource_manager: Sequence) -> None:
    rm, databases_mock, _, _, _ = configure_resource_manager
    databases_mock.create.side_effect = RuntimeError("database already exists")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "test_database",
        ],
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
) -> None:
    """
    General workflow with different databases and parameters and
    custom callback function for checking the results
    """
    rm, databases_mock, _, _, _ = configure_resource_manager

    databases_mock.get_many.return_value = return_databases

    result = CliRunner(mix_stderr=False).invoke(list, additional_parameters)
    databases_mock.get_many.assert_called_once_with(name_contains=name_contains)

    output_validator(result.stdout)
    assert result.stderr == ""
    assert result.exit_code == 0, "non-zero exit code"


def test_databases_list_happy_path_json(
    configure_resource_manager: Sequence,
) -> None:
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
    databases = [Database("db_name1", ""), Database("db_name2", "")]

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
        [
            "--name",
            "to_drop_database_name",
        ]
        + additional_parameters,
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
        [
            "--name",
            "to_drop_database_name",
        ],
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
        drop, ["--name", "to_drop_database_name", "--yes"]
    )

    databases_mock.get_by_name.assert_called_once_with(name="to_drop_database_name")
    database_mock.delete.assert_called_once_with()

    assert result.stderr != "", "cli should fail with an error message in stderr"
    assert result.exit_code != 0, "non-zero exit code"
