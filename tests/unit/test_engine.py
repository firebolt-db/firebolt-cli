from typing import Callable, Sequence
from unittest import mock

import pytest
from click.testing import CliRunner, Result
from firebolt.common.exception import FireboltError
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)
from pytest_mock import MockerFixture

from firebolt_cli.engine import create, start, status, stop


@pytest.fixture(autouse=True)
def configure_cli_autouse(configure_cli: Callable) -> None:
    configure_cli()


def test_engine_start_missing_name() -> None:
    """
    Name is not provided the engine start command
    """
    result = CliRunner(mix_stderr=False).invoke(
        start,
        [],
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_not_found(configure_resource_manager: Sequence) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    rm, _, _, engines_mock, _ = configure_resource_manager

    engines_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(
        start, "--name not_existing_engine".split()
    )

    engines_mock.get_by_name.assert_called_once_with("not_existing_engine")

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def engine_start_stop_generic(
    command: Callable,
    configure_resource_manager: Sequence,
    state_before_call: EngineStatusSummary,
    state_after_call: EngineStatusSummary,
    nowait: bool,
    check_engine_start_call: bool = False,
    check_engine_stop_call: bool = False,
) -> Result:
    """
    generic start/stop engine procedure check
    """
    rm, _, _, engines_mock, engine_mock = configure_resource_manager

    engines_mock.get_by_name.return_value = engine_mock
    engine_mock.current_status_summary = state_before_call

    engine_mock_after_call = mock.MagicMock()
    engine_mock_after_call.current_status_summary = state_after_call

    engine_mock.start.return_value = engine_mock_after_call
    engine_mock.stop.return_value = engine_mock_after_call

    additional_parameters = ["--nowait"] if nowait else []

    result = CliRunner(mix_stderr=False).invoke(
        command,
        ["--name", "engine_name"] + additional_parameters,
    )

    rm.assert_called_once()
    engines_mock.get_by_name.assert_called_once_with("engine_name")

    if check_engine_start_call:
        engine_mock.start.assert_called_once_with(wait_for_startup=not nowait)
    if check_engine_stop_call:
        engine_mock.stop.assert_called_once_with(wait_for_stop=not nowait)

    return result


def test_engine_start_failed(configure_resource_manager: Sequence) -> None:
    """
    Engine was in stopped state before starting,
    but didn't change the state to running after the start call
    """

    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_happy_path(configure_resource_manager: Sequence) -> None:
    """
    Test the normal behavior
    """

    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_happy_path_nowait(configure_resource_manager: Sequence) -> None:
    """
    Test normal behavior with nowait parameter
    """
    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_wrong_state(configure_resource_manager: Sequence) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=False,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_stop_failed(configure_resource_manager: Sequence) -> None:
    """
    Engine was in running state before starting,
    but the state changed to the failed afterwards
    """

    result = engine_start_stop_generic(
        stop,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        nowait=False,
        check_engine_stop_call=True,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_stop_happy_path(configure_resource_manager: Sequence) -> None:
    """
    Test the normal behavior of engine stopping
    """

    result = engine_start_stop_generic(
        stop,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        nowait=False,
        check_engine_stop_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_status_not_found(configure_resource_manager: Sequence) -> None:
    rm, _, _, engines_mock, _ = configure_resource_manager

    engines_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(
        status, "--name non_existing_engine".split()
    )

    engines_mock.get_by_name.assert_called_once_with(name="non_existing_engine")

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_status(configure_resource_manager: Sequence) -> None:
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager

    engine_mock.current_status_summary = "engine running"
    engines_mock.get_by_name.return_value = engine_mock

    result = CliRunner(mix_stderr=False).invoke(status, "--name engine_name".split())

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")

    assert "engine running" in result.stdout
    assert result.stderr == ""
    assert result.exit_code == 0


def test_engine_create_happy_path(
    mocker: MockerFixture, configure_resource_manager: Sequence
) -> None:
    """
    Test engine create standard workflow
    """
    rm, databases_mock, database_mock, engines_mock, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "engine_name",
            "--database_name",
            "database_name",
            "--spec",
            "C1",
            "--region",
            "us-east-1",
        ],
    )

    databases_mock.get_by_name.assert_called_once()
    engines_mock.create.assert_called_once()

    database_mock.attach_to_engine.assert_called_once()

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""


def test_engine_create_database_not_found(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if the database it is attached to doesn't exist
    """
    rm, databases_mock, _, engines_mock, _ = configure_resource_manager

    databases_mock.get_by_name.side_effect = FireboltError("database not found")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "engine_name",
            "--database_name",
            "database_name",
            "--spec",
            "C1",
            "--region",
            "us-east-1",
        ],
    )

    databases_mock.get_by_name.assert_called_once()
    engines_mock.create.assert_not_called()

    databases_mock.attach_to_engine.assert_not_called()

    assert result.stderr != "", ""
    assert result.exit_code != 0, ""


def test_engine_create_name_taken(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if the engine name is already taken
    """
    rm, databases_mock, _, engines_mock, _ = configure_resource_manager
    engines_mock.create.side_effect = FireboltError("engine name already exists")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "engine_name",
            "--database_name",
            "database_name",
            "--spec",
            "C1",
            "--region",
            "us-east-1",
        ],
    )

    databases_mock.get_by_name.assert_called_once()
    engines_mock.create.assert_called_once()

    databases_mock.attach_to_engine.assert_not_called()

    assert result.stderr != "", ""
    assert result.exit_code != 0, ""


def test_engine_create_binding_failed(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if for some reason binding failed;
    Check, that the database deletion was requested
    """
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager

    database_mock.attach_to_engine.side_effect = FireboltError("binding failed")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "engine_name",
            "--database_name",
            "database_name",
            "--spec",
            "C1",
            "--region",
            "us-east-1",
        ],
    )

    databases_mock.get_by_name.assert_called_once()
    engines_mock.create.assert_called_once()

    engine_mock.delete.assert_called_once()
    databases_mock.attach_to_engine.assert_not_called()

    assert result.stderr != "", ""
    assert result.exit_code != 0, ""


def test_engine_create_happy_path_optional_parameters(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test engine create standard workflow with all optional parameters
    """
    rm, databases_mock, database_mock, engines_mock, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        create,
        [
            "--name",
            "engine_name",
            "--database_name",
            "database_name",
            "--spec",
            "C1",
            "--region",
            "us-east-2",
            "--description",
            "test_description",
            "--type",
            "rw",
            "--scale",
            "23",
            "--auto_stop",
            "893",
            "--warmup",
            "all",
        ],
    )

    databases_mock.get_by_name.assert_called_once()
    engines_mock.create.assert_called_once_with(
        name="engine_name",
        spec="C1",
        region="us-east-2",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
        description="test_description",
    )

    database_mock.attach_to_engine.assert_called_once()

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""
