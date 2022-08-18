from unittest.mock import MagicMock

import pytest

from firebolt_cli.completer import FireboltAutoCompleter


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("", []),
        ("S", ["SELECT", "SET"]),
        ("SOMETHING D", ["DROP"]),
        ("select 1;s", ["SELECT"]),
        ("     in", ["INSERT INTO"]),
        ("select mi", ["MIN(<expr>)"]),
    ],
)
def test_complete(test_input, expected):
    """
    test completer some cases
    """
    cursor = MagicMock()
    completer = FireboltAutoCompleter(cursor)

    document = MagicMock()
    document.text_before_cursor = test_input

    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert set(expected).issubset(suggestions)


def test_complete_table_names():
    """
    test completer with table and column names
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = [["table_name", "column_name", "INT"]]
    completer = FireboltAutoCompleter(cursor)

    document = MagicMock()

    document.text_before_cursor = "tab"
    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert "table_name" in suggestions

    document.text_before_cursor = "SELECT * FROM table_name WHERE col"
    document.text = document.text_before_cursor
    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert "column_name" in suggestions


def test_complete_set_statements():
    """
    test completer with table and column names
    """
    cursor = MagicMock()
    completer = FireboltAutoCompleter(cursor)

    document = MagicMock()

    document.text_before_cursor = "set "
    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert "firebolt_dont_wait_for_upload_to_s3" in suggestions

    document.text_before_cursor = "set max"
    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert "max_parser_depth" in suggestions
