import json

from click.testing import CliRunner

from firebolt_cli.main import main


def test_database_create_drop(configure_cli):
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
            "test_database",
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
    assert output["name"] == "test_database"
    assert output["description"] == "database_description"

    # Trying to create a database with the same name
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "create", "--name", "test_database", "--json"]
    )
    assert result.exit_code != 0

    # Dropping the created database
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "drop", "--name", "test_database", "--yes"]
    )
    assert result.exit_code == 0


def test_database_drop_non_existing(configure_cli):
    """
    Trying to drop non-existing database
    """
    result = CliRunner(mix_stderr=False).invoke(
        main, ["database", "drop", "--name", "non_existing_test_database", "--yes"]
    )

    assert result.exit_code != 0, ""
    assert result.stderr != "", ""
