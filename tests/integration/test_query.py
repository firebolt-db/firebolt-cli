import csv
from typing import Sequence

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main


def query_simple_generic(
    additional_parameters: Sequence[str],
    read_from_stdin: bool,
    query: str,
    check_result: bool = True,
):
    """
    Helper function for executing a query either from stdin or file
    """

    stdin_query = query if read_from_stdin else None

    if not read_from_stdin:
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


@pytest.mark.parametrize("read_from_stdin", [True, False])
def test_query_inputs(
    configure_cli: None, engine_name: str, engine_url: str, read_from_stdin: bool
):
    """
    Execute simple from either stdin or file using engine-name/engine-url
    """
    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        read_from_stdin=read_from_stdin,
        query="SELECT 1;",
    )

    query_simple_generic(
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=read_from_stdin,
        query="SELECT 1;",
    )


@pytest.mark.parametrize("read_from_stdin", [True, False])
def test_query_inputs_multiline(
    configure_cli: None, engine_url: str, read_from_stdin: bool
):
    """
    Execute a multiline query
    """
    query_simple_generic(
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=read_from_stdin,
        query="SELECT\n1;",
    )


@pytest.fixture()
def query_select_csv_table_configuration(engine_url: str):
    """
    Fixture create a table and drops it after execution
    """

    table_name = "test_table_query_select"

    query_simple_generic(
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=False,
        query=f"CREATE FACT TABLE {table_name} "
        "(c_id INT, c_name INT) PRIMARY INDEX c_id;",
    )

    yield

    query_simple_generic(
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=False,
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
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=True,
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (1, 213);",
    )

    query_simple_generic(
        additional_parameters=["--engine-name", engine_name],
        read_from_stdin=False,
        query=f"INSERT INTO {table_name} (c_id, c_name) VALUES (2, 123);",
    )

    result = query_simple_generic(
        additional_parameters=["--engine-name", engine_name, "--csv"],
        read_from_stdin=True,
        query=f"SELECT c_id, c_name FROM {table_name} ORDER BY c_id;",
    )

    output = list(csv.reader(result.stdout.splitlines(), delimiter=","))
    header = output[0]
    data = output[1:]

    assert header == ["c_id", "c_name"]
    assert data == [["1", "213"], ["2", "123"]]


@pytest.mark.parametrize("read_from_stdin", [True, False])
def test_query_incorrect(configure_cli: None, engine_url: str, read_from_stdin: bool):
    """
    Test incorrect query
    """
    result = query_simple_generic(
        additional_parameters=["--engine-url", engine_url],
        read_from_stdin=read_from_stdin,
        query="SELECT select;",
        check_result=False,
    )

    assert result.stderr != ""
    assert result.exit_code != 0
