import json

from firebolt_cli.utils import prepare_execution_result

def test_prepare_execution_result_empty() -> None:
    assert json.loads(prepare_execution_result({}, use_json = True)) == {}
    assert len(prepare_execution_result({}, use_json=False)) == 0


def test_prepare_execution_result() -> None:
    test_data = {"a" : "b", "c" : "d"}
    assert json.loads(prepare_execution_result(test_data, use_json = True)) == test_data

    assert len(prepare_execution_result(test_data, use_json=True).split('\n')) >= len(test_data)
