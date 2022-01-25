import json

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main


def test_database_create_drop(configure_cli: None, database_name: str):
    """
    1. Create the database with json output
    2. Check the output is correct
    3. Try to create the database with the same name, it should
    4. Delete the database
    """

    # Creating a test database
    result = CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "create",
            "--name",
            database_name + "_create_test",
            "--description",
            "database_description",
            "--json",
        ],
    )
    assert result.exit_code == 0

    # Checking the correctness of the json output
    output = json.loads(result.stdout)
    assert "name" in output
    assert "description" in output
    assert output["name"] == database_name + "_create_test"
    assert output["description"] == "database_description"

    # Trying to create a database with the same name
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "create", "--name", database_name + "_create_test", "--json"]
    )
    assert result.exit_code != 0

    # Dropping the created database
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "drop", "--name", database_name + "_create_test", "--yes"]
    )
    assert result.exit_code == 0


def test_database_drop_non_existing(configure_cli: None):
    """
    Trying to drop non-existing database
    """
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "drop", "--name", "non_existing_test_database", "--yes"]
    )

    assert result.exit_code != 0, ""
    assert result.stderr != "", ""


@pytest.fixture()
def test_database_list_setup(database_name: str) -> None:
    # Setup the test
    CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "create",
            "--name",
            database_name + "_list_integration_test1",
            "--json",
        ],
    )

    CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "create",
            "--name",
            database_name + "_list_integration_test2",
            "--json",
        ],
    )

    yield

    # Clean up the test
    CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "drop",
            "--name",
            database_name + "_list_integration_test1",
            "--yes",
        ],
    )

    CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "drop",
            "--name",
            database_name + "_list_integration_test2",
            "--yes",
        ],
    )


def test_database_list(
    database_name: str, configure_cli: None, test_database_list_setup: None
):
    """
    Test database list command with:
        - name_contains: multiple match, exact match, and no match
        - json: check both json and tabular output formats
    """

    def json_to_name_list(output: str):
        return [item["name"] for item in json.loads(output)]

    # Test returning simply the list of databases
    result = CliRunner(mix_stderr=False).invoke(main, ["database", "list"])
    assert result.exit_code == 0, ""

    # Test return the list of databases in json format
    result = CliRunner(mix_stderr=False).invoke(main, ["database", "list", "--json"])
    assert result.exit_code == 0, ""

    output = set(json_to_name_list(result.stdout))
    assert database_name + "_list_integration_test1" in output
    assert database_name + "_list_integration_test2" in output

    # Test name contains
    result = CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "list",
            "--json",
            "--name-contains",
            database_name + "_list_integration_test",
        ],
    )
    assert len(json_to_name_list(result.stdout)) == 2

    # Test name contains exact name
    result = CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "list",
            "--json",
            "--name-contains",
            database_name + "_list_integration_test1",
        ],
    )
    assert len(json_to_name_list(result.stdout)) == 1

    # Test name contains non existing
    result = CliRunner(mix_stderr=False).invoke(
        main,
        [
            "database",
            "list",
            "--json",
            "--name-contains",
            database_name + "_non_existing_database",
        ],
    )
    assert len(json_to_name_list(result.stdout)) == 0
