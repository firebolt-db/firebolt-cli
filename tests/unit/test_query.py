import csv
import unittest.mock
from typing import Callable, Optional
from unittest import mock

import pytest
from click.testing import CliRunner
from firebolt.common.exception import FireboltError
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.query import query


@pytest.fixture(autouse=True)
def configure_cli_auto(configure_cli):
    return configure_cli()


def test_query_stdin_file_ambiguity(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    If both query from the file and query from stdin
    are provided, cli should return an error
    """

    fs.create_file("path_to_file.sql")
    result = CliRunner(mix_stderr=False).invoke(
        query,
        [
            "--file",
            "path_to_file.sql",
        ],
        input="query from stdin",
    )

    assert result.stderr != "", "error message is missing"
    assert (
        result.exit_code != 0
    ), "the execution should fail, but cli returned success code"


def test_query_file_missing(mocker: MockerFixture) -> None:
    """
    If sql file doesn't exist, the cli should return an error
    """
    result = CliRunner(mix_stderr=False).invoke(
        query,
        [
            "--file",
            "path_to_file.sql",
        ],
        input=None,
    )

    assert result.stderr != "", "error message is missing"
    assert (
        result.exit_code != 0
    ), "the execution should fail, but cli returned success code"


def query_generic_test(
    mocker: MockerFixture,
    additional_parameter: str,
    check_output_callback: Callable[[str], None],
    expected_sql: str,
    input: Optional[str],
    cursor_mock,
) -> None:
    """
    test sql execution, either sql read from input or from parameters
    """
    cursor_mock.fetchall.return_value = [
        ["test", "test1"],
        ["test2", "test3"],
        ["data1", "data2"],
    ]

    headers = [mock.Mock(), mock.Mock()]
    for header_mock, header_name in zip(headers, ["name1", "name2"]):
        header_mock.name = header_name

    cursor_mock.description = headers

    result = CliRunner().invoke(
        query,
        [
            "--database-name",
            "test_database",
            "--engine-name",
            "engine-name",
        ]
        + additional_parameter,
        input=input,
    )

    check_output_callback(result.stdout)

    cursor_mock.execute.assert_called_once_with(expected_sql)

    assert result.exit_code == 0


def test_query_csv_output(mocker: MockerFixture, cursor_mock) -> None:
    """
    test sql execution with --csv parameter, and check the csv correctness
    """

    def check_csv_correctness(output: str) -> None:
        try:
            csv.Sniffer().sniff(output)
        except csv.Error:
            assert False, "output csv is incorrectly formatted"

    query_generic_test(
        mocker,
        ["--csv"],
        check_csv_correctness,
        expected_sql="query from input;",
        input="query from input;",
        cursor_mock=cursor_mock,
    )


def test_query_tabular_output(mocker: MockerFixture, cursor_mock) -> None:
    """
    test sql execution and check the tabular output correctness
    """

    def check_tabular_correctness(output: str) -> None:
        assert len(output) != ""

    query_generic_test(
        mocker,
        [],
        check_tabular_correctness,
        expected_sql="query from input;",
        input="query from input;",
        cursor_mock=cursor_mock,
    )


def test_query_file(mocker: MockerFixture, fs: FakeFilesystem, cursor_mock) -> None:
    """
    test querying from file (with multiple lines);
    """
    fs.create_file("path_to_file.sql", contents="query from file\nsecond line")

    query_generic_test(
        mocker,
        ["--file", "path_to_file.sql"],
        lambda x: None,
        expected_sql="query from file\nsecond line",
        input=None,
        cursor_mock=cursor_mock,
    )


def test_connection_error(mocker: MockerFixture) -> None:
    """
    If the firebolt.db.connect raise an exception, cli should handle it properly
    """
    connect_function_mock = mocker.patch(
        "firebolt_cli.query.connect", side_effect=FireboltError("mocked error")
    )

    result = CliRunner(mix_stderr=False).invoke(
        query,
        [
            "--database-name",
            "test_database",
            "--engine-name",
            "engine-name",
        ],
        input="some sql",
    )

    connect_function_mock.assert_called_once()
    assert result.stderr != "", "error message is missing"
    assert (
        result.exit_code != 0
    ), "the execution should fail, but cli returned success code"


def test_sql_execution_error(mocker: MockerFixture, cursor_mock) -> None:
    cursor_mock.attach_mock(
        unittest.mock.Mock(side_effect=FireboltError("sql execution failed")), "execute"
    )

    result = CliRunner(mix_stderr=False).invoke(
        query,
        [
            "--database-name",
            "test_database",
            "--engine-name",
            "engine-name",
        ],
        input="wrong sql;",
    )

    cursor_mock.execute.assert_called_once_with("wrong sql;")

    assert result.stderr != "", "error message is missing"
    assert (
        result.exit_code != 0
    ), "the execution should fail, but cli returned success code"
