import os
import sys
import unittest
from unittest import mock

from firebolt.common.exception import FireboltError
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput

from firebolt_cli.query import enter_interactive_session


def test_interactive_immediate_stop() -> None:
    """
    Enter interactive shell and send EOF immediately
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    if sys.platform.startswith("win"):
        inp.close()
    else:
        os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_send_empty() -> None:
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

    if sys.platform.startswith("win"):
        inp.close()
    else:
        os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_not_called()


def test_interactive_multiple_requests() -> None:
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

    if sys.platform.startswith("win"):
        inp.close()
    else:
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


def test_interactive_raise_error() -> None:
    """
    Test wrong sql, raise an error, but the execution continues
    """
    inp = create_pipe_input()
    cursor_mock = unittest.mock.MagicMock()

    cursor_mock.attach_mock(
        unittest.mock.Mock(side_effect=FireboltError("sql execution failed")), "execute"
    )

    inp.send_text("wrong sql;\n")

    if sys.platform.startswith("win"):
        inp.close()
    else:
        os.close(inp._w)

    with create_app_session(input=inp, output=DummyOutput()):
        enter_interactive_session(cursor_mock, False)

    cursor_mock.execute.assert_called_once_with("wrong sql")
