import csv
import os
import unittest.mock
from typing import Callable, Optional
from unittest import mock

from click.testing import CliRunner
from firebolt.common.exception import FireboltError
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.query import enter_interactive_session, query


def test_query_stdin_file_ambiguity(
    mocker: MockerFixture, fs: FakeFilesystem, configure_cli
) -> None:
    """
    If both query from the file and query from stdin
    are provided, cli should return an error
    """
    configure_cli()
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


def test_query_file_missing(mocker: MockerFixture, configure_cli) -> None:
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


def test_query_csv_output(mocker: MockerFixture, cursor_mock, configure_cli) -> None:
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
        mocker,
        ["--csv"],
        check_csv_correctness,
        expected_sql="query from input;",
        input="query from input;",
        cursor_mock=cursor_mock,
    )


def test_query_tabular_output(
    mocker: MockerFixture, cursor_mock, configure_cli
) -> None:
    """
    test sql execution and check the tabular output correctness
    """
    configure_cli()

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


def test_query_file(
    mocker: MockerFixture, fs: FakeFilesystem, cursor_mock, configure_cli
) -> None:
    """
    test querying from file (with multiple lines);
    """
    configure_cli()

    fs.create_file("path_to_file.sql", contents="query from file\nsecond line")

    query_generic_test(
        mocker,
        ["--file", "path_to_file.sql"],
        lambda x: None,
        expected_sql="query from file\nsecond line",
        input=None,
        cursor_mock=cursor_mock,
    )


def test_connection_error(mocker: MockerFixture, configure_cli) -> None:
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


def test_sql_execution_error(mocker: MockerFixture, cursor_mock, configure_cli) -> None:
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


def test_interactive_immediate_stop(mocker: MockerFixture) -> None:
    """
    Enter interactive shell and send EOF immediately
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_send_empty(mocker: MockerFixture) -> None:
    """
    Empty strings should not be sent to the cursor
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    inp.send_text(" \n")
    inp.send_text("\t \n")
    inp.send_text("   ;;;;   \n")
    inp.send_text(";\n")
    inp.send_text(";;\n")
    os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_multiple_requests(mocker: MockerFixture) -> None:
    """
    Test interactive sql happy path,
    multiple requests are passed one by one to the cursor
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    inp.send_text("SELECT 1;\n")
    inp.send_text("SELECT 2;\n")
    inp.send_text("SELECT 3;\n")
    inp.send_text("SELECT 4;\n")
    os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_has_calls(
        [
            mock.call("SELECT 1"),
            mock.call("SELECT 2"),
            mock.call("SELECT 3"),
            mock.call("SELECT 4"),
        ],
        any_order=False,
    )

    assert cursor_mock.execute.call_count == 4


def test_interactive_raise_error(mocker: MockerFixture) -> None:
    """
    Test wrong sql, raise an error, but the execution continues
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    cursor_mock.attach_mock(
        unittest.mock.Mock(side_effect=FireboltError("sql execution failed")), "execute"
    )

    inp.send_text("wrong sql;\n")
    os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_called_once_with("wrong sql")
