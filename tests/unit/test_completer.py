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
    ],
)
def test_complete_keywords(test_input, expected):
    """
    test completer some cases
    """
    completer = FireboltAutoCompleter()

    document = MagicMock()
    document.text = test_input

    suggestions = {i.text for i in completer.get_completions(document, None)}
    assert set(expected).issubset(suggestions)
