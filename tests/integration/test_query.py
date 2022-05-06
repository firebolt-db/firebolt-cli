import csv
from typing import Sequence

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main
from firebolt_cli.utils import construct_resource_manager


def query_simple_generic(
    additional_parameters: Sequence[str],
    input_type: str,
    query: str,
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

    result = CliRunner(mix_stderr=False).invoke(
        main,
        [
            "query",
        ]
        + additional_parameters,
        input=stdin_query,
    )

    assert result.exit_code == 0 or not check_result

    return result


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_inputs(
    configure_cli: None, engine_name: str, engine_url: str, input_type: str
):
    """
    Execute simple from either stdin or file using both engine-name and engine-url
    """
    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type=input_type,
        query="SELECT 1;",
    )

    query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type=input_type,
        query="SELECT 1;",
    )


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_inputs_multiline(configure_cli: None, engine_url: str, input_type: str):
    """
    Execute a multiline query
    """
    query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type=input_type,
        query="SELECT\n1;",
    )


@pytest.fixture()
def query_select_csv_table_configuration(engine_url: str):
    """
    Fixture create a table and drops it after execution
    """

    table_name = "test_table_query_select"

    query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type="cli",
        query=f"CREATE FACT TABLE {table_name} "
        "(c_id INT, c_name INT) PRIMARY INDEX c_id;",
    )

    yield

    query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type="stdin",
        query=f"DROP TABLE {table_name};",
    )


def test_query_select_csv(
    configure_cli: None,
    engine_name: str,
    engine_url: str,
    query_select_csv_table_configuration: None,
):
    """
    Create a table, insert values and select them.
    Validate that the returned csv is correct
    """

    table_name = "test_table_query_select"

    query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type="file",
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (1, 213);",
    )

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        input_type="cli",
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (2, 123);",
    )

    result = query_simple_generic(
        additional_parameters=["--engine-name", engine_name, "--csv"],
        input_type="stdin",
        query=f"SELECT c_id, c_name FROM {table_name} ORDER BY c_id;",
    )

    output = list(csv.reader(result.stdout.splitlines(), delimiter=","))
    header = output[0]
    data = output[1:]

    assert header == ["c_id", "c_name"]
    assert data == [["1", "213"], ["2", "123"]]


@pytest.mark.parametrize("input_type", ["stdin", "cli", "file"])
def test_query_incorrect(configure_cli: None, engine_url: str, input_type: str):
    """
    Test incorrect query
    """
    result = query_simple_generic(
        additional_parameters=["--engine-name", engine_url],
        input_type=input_type,
        query="SELECT select;",
        check_result=False,
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_incorrect_credentials(configure_cli: None, engine_name: str):
    """
    Test incorrect credentials on query
    """

    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"query --engine-name {engine_name}".split(),
        input="SELECT 1;",
        env={"FIREBOLT_PASSWORD": "incorrect_password"},
    )

    assert "401 Unauthorized" in result.stderr
    assert "Traceback" not in result.stderr

    assert result.exit_code != 0


def test_query_account_name(configure_cli: None, engine_name: str, account_name: str):
    """
    Test account name correct/incorrect
    """

    query_simple_generic(
        additional_parameters=[
            "--engine-name",
            engine_name,
            "--account-name",
            account_name,
        ],
        input_type="stdin",
        query="SELECT 1;",
        check_result=True,
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
    )
    assert "Account" in result.stderr
    assert "does not exist" in result.stderr
    assert "firebolt_non_existing" in result.stderr

    assert result.exit_code != 0


def test_query_with_access_token(
    username: str, password: str, account_name: str, api_endpoint: str, engine_name: str
):
    """ """
    access_token = construct_resource_manager(
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
        access_token=None,
    ).client.auth._token

    query_simple_generic(
        additional_parameters=[
            "--engine-name",
            engine_name,
            "--account-name",
            account_name,
            "--access-token",
            access_token,
        ],
        input_type="file",
        query="SELECT 1;",
        check_result=True,
    )
