import csv
import unittest.mock
from collections import namedtuple
from typing import Callable, Optional, Sequence
from unittest import mock

from click.testing import CliRunner
from firebolt.common.exception import FireboltError
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.query import query


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

    assert "both are specified" in result.stderr, "error message is incorrect"
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
        expected_sql="query from input;",
        input="query from input;",
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


def test_connection_error(mocker: MockerFixture, configure_cli: Callable) -> None:
    """
    If the firebolt.db.connect raise an exception, cli should handle it properly
    """
    configure_cli()

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
    expected_sql = "SELECT * FROM t1; SELECT * FROM t2"

    result = CliRunner().invoke(
        query,
        "--engine-name engine-name".split(),
        input=expected_sql,
    )
    assert result.exit_code == 0

    assert cursor_mock.nextset.call_count == 2
    assert cursor_mock.fetchall.call_count == 2

    cursor_mock.execute.assert_called_once_with(expected_sql)


def test_query_default_engine(
    mocker: MockerFixture, configure_cli: Callable, cursor_mock: unittest.mock.Mock
):
    """
    Monkey path get_default_database_engine function, and check,
    that it is called if the engine-name is not provided to the query
    """
    configure_cli()

    construct_resource_manager_mock = mocker.patch(
        "firebolt_cli.query.construct_resource_manager"
    )
    default_database_engine_mock = mocker.patch(
        "firebolt_cli.query.get_default_database_engine"
    )

    _Engine = namedtuple("Engine", "name")
    default_database_engine_mock.return_value = _Engine("default_engine_name")

    query_generic_test(
        [],
        None,
        expected_sql="SELECT 1;",
        input="SELECT 1;",
        cursor_mock=cursor_mock,
    )

    construct_resource_manager_mock.assert_called_once()
    default_database_engine_mock.assert_called_once()
