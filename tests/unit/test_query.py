import csv
import unittest.mock
from typing import Callable, Optional, Sequence
from unittest import mock

import pytest
from click.testing import CliRunner
from firebolt.async_db.cursor import Statistics
from firebolt.common.exception import FireboltError
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.query import format_time, query


def test_query_stdin_file_ambiguity(
    fs: FakeFilesystem, configure_cli: Callable
) -> None:
    """
    If both query from the file and query from stdin
    are provided, cli should return an error
    """
    configure_cli()

    fs.create_file("path_to_file.sql", contents="SELECT 1")
    result = CliRunner(mix_stderr=False).invoke(
        query,
        [
            "--file",
            "path_to_file.sql",
        ],
        input="query from stdin",
    )

    assert "Multiple are specified" in result.stderr, "error message is incorrect"
    assert (
        result.exit_code != 0
    ), "the execution should fail, but cli returned success code"


def test_query_file_missing(configure_cli: Callable) -> None:
    """
    If sql file doesn't exist, the cli should return an error
    """

    configure_cli()
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
    additional_parameter: Sequence[str],
    check_output_callback: Optional[Callable[[str], None]],
    expected_sql: str,
    input: Optional[str],
    cursor_mock: unittest.mock.Mock,
) -> None:
    """
    test sql execution, either sql read from input or from parameters
    """

    cursor_mock.nextset.return_value = None
    cursor_mock.fetchall.return_value = [
        ["test", "test1"],
        ["test2", "test3"],
        ["data1", "data2"],
    ]

    headers = [mock.Mock(), mock.Mock()]
    for header_mock, header_name in zip(headers, ["name1", "name2"]):
        header_mock.name = header_name

    cursor_mock.description = headers
    cursor_mock.statistics = Statistics(
        elapsed=0.2,
        rows_read=2,
        bytes_read=32132,
        time_before_execution=0.1,
        time_to_execute=0.1,
        scanned_bytes_cache=None,
        scanned_bytes_storage=None,
    )

    result = CliRunner().invoke(
        query,
        [
            "--database-name",
            "test_database",
        ]
        + additional_parameter,
        input=input,
    )

    if check_output_callback:
        check_output_callback(result.stdout)

    cursor_mock.execute.assert_called_once_with(expected_sql)

    assert result.exit_code == 0


def test_query_csv_output(
    cursor_mock: unittest.mock.Mock, configure_cli: Callable
) -> None:
    """
    test sql execution with --csv parameter, and check the csv correctness
    """
    configure_cli()

    def check_csv_correctness(output: str) -> None:
        try:
            csv.Sniffer().sniff(output)
        except csv.Error:
            assert False, "output csv is incorrectly formatted"

    query_generic_test(
        ["--csv", "--engine-name", "engine-name"],
        check_csv_correctness,
        expected_sql="SELECT 1;",
        input="SELECT 1;",
        cursor_mock=cursor_mock,
    )


def test_query_tabular_output(
    cursor_mock: unittest.mock.Mock, configure_cli: Callable
) -> None:
    """
    test sql execution and check the tabular output correctness
    """

    configure_cli()

    def check_tabular_correctness(output: str) -> None:
        assert len(output) != 0
        assert "Firebolt elapsed time : 0.20s" in output
        assert "Scanned bytes         : 31.38 KB" in output

    query_generic_test(
        ["--engine-name", "engine-name"],
        check_tabular_correctness,
        expected_sql="query from input;",
        input="query from input;",
        cursor_mock=cursor_mock,
    )


def test_query_file(
    fs: FakeFilesystem, cursor_mock: unittest.mock.Mock, configure_cli: Callable
) -> None:
    """
    test querying from file (with multiple lines);
    """
    configure_cli()

    fs.create_file("path_to_file.sql", contents="query from file\nsecond line")

    query_generic_test(
        ["--file", "path_to_file.sql", "--engine-name", "engine_name"],
        lambda x: None,
        expected_sql="query from file\nsecond line",
        input=None,
        cursor_mock=cursor_mock,
    )


def test_query_argument(
    fs: FakeFilesystem, cursor_mock: unittest.mock.Mock, configure_cli: Callable
) -> None:
    """
    test querying from command line argument;
    """
    configure_cli()

    query_generic_test(
        [
            "--engine-name",
            "engine_name",
            "--sql",
            "query from command-line\nsecond line",
        ],
        lambda x: None,
        expected_sql="query from command-line\nsecond line",
        input=None,
        cursor_mock=cursor_mock,
    )


def test_connection_error(mocker: MockerFixture, configure_cli: Callable) -> None:
    """
    If the firebolt.db.connect raise an exception, cli should handle it properly
    """
    configure_cli()

    connect_function_mock = mocker.patch(
        "firebolt_cli.query.create_connection",
        side_effect=FireboltError("mocked error"),
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


def test_sql_execution_error(
    cursor_mock: unittest.mock.Mock, configure_cli: Callable
) -> None:
    configure_cli()

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


def test_sql_execution_multiline(
    cursor_mock: unittest.mock.Mock, configure_cli: Callable
):
    """

    :param cursor_mock:
    :param configure_cli:
    :return:
    """
    configure_cli()

    cursor_mock.nextset.side_effect = [True, None]

    cursor_mock.fetchall.return_value = [
        ["test", "test1"],
        ["test2", "test3"],
        ["data1", "data2"],
    ]

    headers = [mock.Mock(), mock.Mock()]
    for header_mock, header_name in zip(headers, ["name1", "name2"]):
        header_mock.name = header_name

    cursor_mock.description = headers
    cursor_mock.statistics = None

    result = CliRunner().invoke(
        query,
        "--engine-name engine-name".split(),
        input="SELECT * FROM t1;SELECT * FROM t2;",
    )
    assert result.exit_code == 0

    assert cursor_mock.nextset.call_count == 2
    assert cursor_mock.fetchall.call_count == 2

    assert cursor_mock.execute.mock_calls == [
        mock.call("SELECT * FROM t1;"),
        mock.call("SELECT * FROM t2;"),
    ]


@pytest.mark.parametrize(
    "execution_time, formatted_time",
    [
        (0.23, "0.23s"),
        (3.12, "3.12s"),
        (345.03, "5m 45.03s"),
        (3600.32, "1h 0m 0.32s"),
        (7264.3, "2h 1m 4.30s"),
    ],
)
def test_format_time(execution_time: float, formatted_time: str):
    """
    test format time of query execution
    """
    assert format_time(execution_time) == formatted_time
