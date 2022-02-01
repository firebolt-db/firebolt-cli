import json

import pytest

from firebolt_cli.utils import (
    prepare_execution_result_line,
    prepare_execution_result_table,
)


def test_prepare_execution_empty() -> None:
    headers = ["name0", "name1", "name2", "name3"]
    assert json.loads(prepare_execution_result_table([], headers, use_json=True)) == []
    assert len(prepare_execution_result_table([], headers, use_json=False)) > 0


def test_prepare_execution_single() -> None:
    data = [0, 1, 2, 3]
    headers = ["name0", "name1", "name2", "name3"]

    j = json.loads(prepare_execution_result_line(data, headers, use_json=True))
    for header in headers:
        assert header in j

    assert (
        len(prepare_execution_result_line(data, headers, use_json=False).split("\n"))
        > 0
    )


def test_prepare_execution_multiple() -> None:
    data = [[0, 1, 2, 3], [4, 5, 6, 7]]
    headers = ["name0", "name1", "name2", "name3"]

    j = json.loads(prepare_execution_result_table(data, headers, use_json=True))
    assert len(j) == 2

    for header in headers:
        assert header in j[0]
        assert header in j[1]

    assert (
        len(prepare_execution_result_table(data, headers, use_json=False).split("\n"))
        > 0
    )


def test_prepare_execution_wrong_header() -> None:
    data = [[0, 1, 2, 3], [4, 5, 6, 7]]
    headers = ["name0", "name1", "name2", "name3"]
    wrong_data = [[0, 1, 2, 3], [4, 5, 6]]
    wrong_headers = ["name0", "name1", "name2"]

    with pytest.raises(ValueError):
        prepare_execution_result_table(wrong_data, headers, use_json=True)

    with pytest.raises(ValueError):
        prepare_execution_result_table(data, wrong_headers, use_json=True)

    with pytest.raises(ValueError):
        prepare_execution_result_line(wrong_data, headers, use_json=False)

    with pytest.raises(ValueError):
        prepare_execution_result_line(data, wrong_headers, use_json=False)
