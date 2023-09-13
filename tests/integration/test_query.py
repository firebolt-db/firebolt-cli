import csv
from typing import Sequence

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main


def query_simple_generic(
    additional_parameters: Sequence[str],
    input_type: str,
    query: str,
    cli_runner: CliRunner,
    check_result: bool = True,
):
    """
    Helper function for executing a query either from stdin or file
    """

    stdin_query = None
    if input_type == "stdin":
        stdin_query = query
    elif input_type == "cli":
        additional_parameters.extend(["--sql", query])
    elif input_type == "file":
        additional_parameters.extend(["--file", "query.sql"])
        with open("query.sql", "w") as f:
            f.write(query)

    result = cli_runner.invoke(
        main,
        [
            "query",
        ]
        + additional_parameters,
        input=stdin_query,
    )

    assert result.exit_code == 0 or not check_result, result.stderr

    return result


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_inputs(cli_runner: CliRunner, engine_name: str, input_type: str):
    """
    Execute simple from either stdin or file using both engine-name and engine-url
    """
    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type=input_type,
        query="SELECT 1;",
        cli_runner=cli_runner,
    )


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_inputs_multiline(
    cli_runner: CliRunner, engine_name: str, input_type: str
):
    """
    Execute a multiline query
    """
    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type=input_type,
        query="SELECT\n1;",
        cli_runner=cli_runner,
    )


@pytest.fixture()
def query_select_csv_table_configuration(cli_runner: CliRunner, engine_name: str):
    """
    Fixture create a table and drops it after execution
    """

    table_name = "test_table_query_select"

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type="cli",
        query=f"CREATE FACT TABLE {table_name} "
        "(c_id INT, c_name INT) PRIMARY INDEX c_id;",
        cli_runner=cli_runner,
    )

    yield

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type="stdin",
        query=f"DROP TABLE {table_name};",
        cli_runner=cli_runner,
    )


def test_query_select_csv(
    cli_runner: CliRunner,
    engine_name: str,
    query_select_csv_table_configuration: None,
):
    """
    Create a table, insert values and select them.
    Validate that the returned csv is correct
    """

    table_name = "test_table_query_select"

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type="file",
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (1, 213);",
        cli_runner=cli_runner,
    )

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type="cli",
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (2, 123);",
        cli_runner=cli_runner,
    )

    result = query_simple_generic(
        additional_parameters=["--engine-name", engine_name, "--csv"],
        input_type="stdin",
        query=f"SELECT c_id, c_name FROM {table_name} ORDER BY c_id;",
        cli_runner=cli_runner,
    )

    output = list(csv.reader(result.stdout.splitlines(), delimiter=","))
    header = output[0]
    data = output[1:]

    assert header == ["c_id", "c_name"]
    assert data == [["1", "213"], ["2", "123"]]


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_incorrect(cli_runner: CliRunner, engine_name: str, input_type: str):
    """
    Test incorrect query
    """
    result = query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type=input_type,
        query="SELECT select;",
        check_result=False,
        cli_runner=cli_runner,
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_incorrect_credentials(cli_runner: CliRunner, engine_name: str):
    """
    Test incorrect credentials on query
    """

    result = cli_runner.invoke(
        main,
        f"query --engine-name {engine_name}".split(),
        input="SELECT 1;",
        env={"FIREBOLT_CLIENT_SECRET": "incorrect_secret"},
    )

    assert "401 Unauthorized" in result.stderr
    assert "Traceback" not in result.stderr

    assert result.exit_code != 0


def test_query_happy_path(cli_runner: CliRunner, engine_name: str, account_name: str):
    """
    Test account name correct/incorrect
    """

    query_simple_generic(
        additional_parameters=[
            "--engine-name",
            engine_name,
        ],
        input_type="stdin",
        query="SELECT 1;",
        check_result=True,
        cli_runner=cli_runner,
    )

    result = query_simple_generic(
        additional_parameters=[
            "--engine-name",
            engine_name,
            "--account-name",
            "firebolt_non_existing",
        ],
        input_type="cli",
        query="SELECT 1;",
        check_result=False,
        cli_runner=cli_runner,
    )
    assert "Account" in result.stderr
    assert "does not exist" in result.stderr
    assert "firebolt_non_existing" in result.stderr

    assert result.exit_code != 0
