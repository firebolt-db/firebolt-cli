import unittest
from unittest import mock

import pytest
from firebolt.common.exception import FireboltError
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from pytest_mock import MockerFixture

from firebolt_cli.query import (
    INTERNAL_COMMANDS,
    enter_interactive_session,
    process_internal_command,
)


def test_interactive_immediate_stop() -> None:
    """
    Enter interactive shell and send EOF immediately
    """
    with create_pipe_input() as inp:
        cursor_mock = unittest.mock.MagicMock()
        inp.send_text(".quit\n")

        with create_app_session(input=inp, output=DummyOutput()):
            enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_send_empty() -> None:
    """
    Empty strings should not be sent to the cursor
    """
    with create_pipe_input() as inp:
        cursor_mock = unittest.mock.MagicMock()

        inp.send_text(" \n")
        inp.send_text("\t \n")
        inp.send_text("   ;;;;   \n")
        inp.send_text(";\n")
        inp.send_text(";;\n")
        inp.send_text(".quit\n")

        with create_app_session(input=inp, output=DummyOutput()):
            enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_multiple_requests() -> None:
    """
    Test interactive sql happy path,
    multiple requests are passed one by one to the cursor
    """
    with create_pipe_input() as inp:
        connection_mock = unittest.mock.MagicMock()
        cursor_mock = unittest.mock.MagicMock()
        cursor_mock.statistics = None
        cursor_mock.data = []
        cursor_mock.description = []
        connection_mock.cursor.return_value = cursor_mock

        cursor_mock.nextset.return_value = None

        inp.send_text("SELECT 1;\n")
        inp.send_text("SELECT 2;\n")
        inp.send_text("SELECT 3;\n")
        inp.send_text("SELECT 4;\n")
        inp.send_text(".quit\n")

        with create_app_session(input=inp, output=DummyOutput()):
            enter_interactive_session(connection_mock, False)

        cursor_mock.execute.assert_has_calls(
            [
                mock.call("SELECT 1"),
                mock.call("SELECT 2"),
                mock.call("SELECT 3"),
                mock.call("SELECT 4"),
            ],
            any_order=False,
        )

    assert cursor_mock.execute.call_count == 7


def test_interactive_raise_error() -> None:
    """
    Test wrong sql, raise an error, but the execution continues
    """
    connection_mock = unittest.mock.MagicMock()
    cursor_mock = unittest.mock.MagicMock()
    cursor_mock.statistics = None
    connection_mock.cursor.return_value = cursor_mock

    cursor_mock.attach_mock(
        unittest.mock.Mock(side_effect=FireboltError("sql execution failed")), "execute"
    )

    with create_pipe_input() as inp:
        inp.send_text("wrong sql;\n")
        inp.send_text(".quit\n")

        with create_app_session(input=inp, output=DummyOutput()):
            enter_interactive_session(connection_mock, False)

    cursor_mock.execute.assert_called_with("wrong sql")


def test_process_internal_command():
    """
    test process internal command exit or tables
    """
    with pytest.raises(EOFError):
        process_internal_command(".quit")

    with pytest.raises(EOFError):
        process_internal_command(".exit")

    with pytest.raises(ValueError):
        process_internal_command(".make")

    assert "show tables;" == process_internal_command(".tables").lower()


def test_process_internal_command_help(capsys):
    """
    test process .help
    """
    process_internal_command(".help")
    captured = capsys.readouterr()

    assert len(captured.out.split("\n")) >= 3
    for command in INTERNAL_COMMANDS:
        assert command in captured.out


def test_interactive_multi_statement(mocker: MockerFixture) -> None:
    """
    Test interactive sql happy path,
    multistatement is passed
    """
    execute_and_print_mock = mocker.patch("firebolt_cli.query.execute_and_print")
    with create_pipe_input() as inp:
        connection_mock = unittest.mock.MagicMock()
        cursor_mock = unittest.mock.MagicMock()
        connection_mock.cursor.return_value = cursor_mock

        cursor_mock.nextset.side_effect = [True, None]
        cursor_mock.description = None

        inp.send_text("SELECT 1; SELECT 2;\n")
        inp.send_text(".exit\n")

        with create_app_session(input=inp, output=DummyOutput()):
            enter_interactive_session(connection_mock, False)

    execute_and_print_mock.assert_called_once_with(
        cursor_mock, "SELECT 1; SELECT 2", False
    )
