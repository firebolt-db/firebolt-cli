import json

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main


def test_database_create_drop(configure_cli: None, database_name: str):
    """
    1. Create the database with json output
    2. Check the output is correct
    3. Call database describe and check the output
    4. Try to create the database with the same name, it should
    5. Delete the database
    """

    database_name = f"{database_name}_create_test3"
    # Creating a test database
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database create --name {database_name} "
        f"--description database_description --json".split(),
    )
    assert result.exit_code == 0

    # Checking the correctness of the json output
    output = json.loads(result.stdout)
    assert "name" in output
    assert "description" in output
    assert output["name"] == database_name
    assert output["description"] == "database_description"

    # Getting the description of the database
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database describe --name {database_name} --json".split(),
    )
    assert result.stderr == ""
    assert result.exit_code == 0
    database_description = json.loads(result.stdout)

    assert "name" in database_description
    assert "description" in database_description
    assert "data_size" in database_description
    assert "attached_engine_names" in database_description
    assert "region" in database_description

    assert database_description["name"] == database_name
    assert database_description["description"] == "database_description"
    assert database_description["attached_engine_names"] == []

    # Trying to create a database with the same name
    result = CliRunner(mix_stderr=False).invoke(
        main, f"database create --name {database_name} --json".split()
    )
    assert result.exit_code != 0

    # Dropping the created database
    result = CliRunner(mix_stderr=False).invoke(
        main, f"database drop --name {database_name} --yes".split()
    )
    assert result.exit_code == 0


def test_database_description_not_existing(configure_cli: None, database_name: str):
    """
    database describe should fail if database_name doesn't exist
    """
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database describe --name {database_name}_not_existing --json".split(),
    )
    assert result.stderr != ""
    assert result.exit_code != 0


def test_database_drop_non_existing(configure_cli: None):
    """
    Trying to drop non-existing database
    """
    result = CliRunner(mix_stderr=False).invoke(
        main, f"database drop --name non_existing_test_database --yes".split()
    )

    assert result.exit_code != 0, ""
    assert result.stderr != "", ""


@pytest.fixture()
def test_database_list_setup(database_name: str) -> None:
    # Setup the test
    CliRunner(mix_stderr=False).invoke(
        main,
        f"database create --name {database_name}_list_integration_test1 --json".split(),
    )

    CliRunner(mix_stderr=False).invoke(
        main,
        f"database create --name {database_name}_list_integration_test2 --json".split(),
    )

    yield

    # Clean up the test
    CliRunner(mix_stderr=False).invoke(
        main,
        f"database drop --name {database_name}_list_integration_test1 --yes".split(),
    )

    CliRunner(mix_stderr=False).invoke(
        main,
        f"database drop --name {database_name}_list_integration_test2 --yes".split(),
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
    result = CliRunner(mix_stderr=False).invoke(main, "database list".split())
    assert result.exit_code == 0, ""

    # Test return the list of databases in json format
    result = CliRunner(mix_stderr=False).invoke(main, "database list --json".split())
    assert result.exit_code == 0, ""

    output = set(json_to_name_list(result.stdout))
    assert database_name + "_list_integration_test1" in output
    assert database_name + "_list_integration_test2" in output

    # Test name contains
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database list --json "
        f"--name-contains {database_name}_list_integration_test".split(),
    )
    assert len(json_to_name_list(result.stdout)) == 2

    # Test name contains exact name
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database list --json "
        f"--name-contains {database_name}_list_integration_test1".split(),
    )
    assert len(json_to_name_list(result.stdout)) == 1

    # Test name contains non existing
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"database list --json "
        f"--name-contains {database_name}_non_existing_database".split(),
    )
    assert len(json_to_name_list(result.stdout)) == 0
