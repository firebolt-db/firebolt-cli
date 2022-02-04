import unittest.mock
from typing import Callable, Sequence
from unittest import mock

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner, Result
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.configure import configure
from firebolt_cli.engine import create, start, status, stop, update


@pytest.fixture(autouse=True)
def configure_cli(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    runner.invoke(
        configure,
        [
            "--username",
            "username",
            "--account-name",
            "account_name",
            "--database-name",
            "database_name",
            "--engine-name",
            "engine_name",
            "--api-endpoint",
            "api_endpoint",
        ],
        input="password",
    )


def test_engine_start_missing_name(mocker: MockerFixture) -> None:
    """
    Name is not provided the engine start command
    """
    result = CliRunner(mix_stderr=False).invoke(
        start,
        [],
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_not_found(mocker: MockerFixture) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    engine_mock = mocker.patch.object(ResourceManager, "engines", create=True)
    engine_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(
        start, "--name not_existing_engine".split()
    )

    rm.assert_called_once()
    engine_mock.get_by_name.assert_called_once_with("not_existing_engine")

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def engine_start_stop_generic(
    command: Callable,
    mocker: MockerFixture,
    state_before_call: EngineStatusSummary,
    state_after_call: EngineStatusSummary,
    nowait: bool,
    check_engine_start_call: bool = False,
    check_engine_stop_call: bool = False,
) -> Result:
    """
    generic start/stop engine procedure check
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)

    engine_mock_before_call = mock.MagicMock()
    engine_mock_before_call.current_status_summary = (
        state_before_call  # EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
    )
    engines_mock.get_by_name.return_value = engine_mock_before_call

    engine_mock_after_call = mock.MagicMock()
    engine_mock_after_call.current_status_summary = (
        state_after_call  # EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED
    )
    engine_mock_before_call.start.return_value = engine_mock_after_call
    engine_mock_before_call.stop.return_value = engine_mock_after_call

    additional_parameters = ["--nowait"] if nowait else []

    result = CliRunner(mix_stderr=False).invoke(
        command,
        ["--name", "broken_engine"] + additional_parameters,
    )

    rm.assert_called_once()
    engines_mock.get_by_name.assert_called_once_with("broken_engine")

    if check_engine_start_call:
        engine_mock_before_call.start.assert_called_once_with(
            wait_for_startup=not nowait
        )
    if check_engine_stop_call:
        engine_mock_before_call.stop.assert_called_once_with(wait_for_stop=not nowait)

    return result


def test_engine_start_failed(mocker: MockerFixture) -> None:
    """
    Engine was in stopped state before starting,
    but didn't change the state to running after the start call
    """

    result = engine_start_stop_generic(
        start,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_happy_path(mocker: MockerFixture) -> None:
    """
    Test the normal behavior
    """

    result = engine_start_stop_generic(
        start,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_happy_path_nowait(mocker: MockerFixture) -> None:
    """
    Test normal behavior with nowait parameter
    """
    result = engine_start_stop_generic(
        start,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_wrong_state(mocker: MockerFixture) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    result = engine_start_stop_generic(
        start,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=False,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_stop_failed(mocker: MockerFixture) -> None:
    """
    Engine was in running state before starting,
    but the state changed to the failed afterwards
    """

    result = engine_start_stop_generic(
        stop,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        nowait=False,
        check_engine_stop_call=True,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_stop_happy_path(mocker: MockerFixture) -> None:
    """
    Test the normal behavior of engine stopping
    """

    result = engine_start_stop_generic(
        stop,
        mocker,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        nowait=False,
        check_engine_stop_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


@pytest.fixture()
def configure_resource_manager(mocker: MockerFixture) -> ResourceManager:
    """
    Configure resource manager mock
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)
    mocker.patch.object(ResourceManager, "engine_revisions", create=True)
    mocker.patch.object(ResourceManager, "instance_types", create=True)

    database_mock = unittest.mock.MagicMock()
    databases_mock.get_by_name.return_value = database_mock

    engine_mock = unittest.mock.MagicMock()
    engines_mock.create.return_value = engine_mock
    engines_mock.get_by_name.return_value = engine_mock

    yield rm, databases_mock, database_mock, engines_mock, engine_mock

    rm.assert_called_once()


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
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager

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

    databases_mock.get_by_name.assert_called_once_with(name="database_name")
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

    database_mock.attach_to_engine.assert_called_once_with(
        engine=engine_mock, is_default_engine=True
    )

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""


def test_engine_status_not_found(mocker: MockerFixture) -> None:
    """
    Test engine status, engine was not found
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)
    engines_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(
        status, "--name non_existing_engine".split()
    )

    engines_mock.get_by_name.assert_called_once_with(name="non_existing_engine")
    rm.assert_called_once()

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_status(mocker: MockerFixture) -> None:
    """
    Test engine status general workflow
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)
    engine_mock = mock.MagicMock()
    engine_mock.current_status_summary = "engine running"
    engines_mock.get_by_name.return_value = engine_mock

    result = CliRunner(mix_stderr=False).invoke(status, "--name engine_name".split())

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")
    rm.assert_called_once()

    assert "engine running" in result.stdout
    assert result.stderr == ""
    assert result.exit_code == 0


def generic_engine_update(configure_resource_manager: Sequence, parameters: str):
    """
    Test engine create standard workflow with all optional parameters
    """
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager
    engine_mock.update.return_value = engine_mock

    result = CliRunner(mix_stderr=False).invoke(update, parameters.split())

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""

    return engine_mock


def test_engine_update_all_parameters(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test engine create standard workflow with all optional parameters
    """

    engine_mock = generic_engine_update(
        configure_resource_manager,
        "--name engine_name --new_engine_name name_of_the_new_engine "
        "--spec C1 --description test_description "
        "--type rw --scale 23 --auto_stop 893 --warmup all",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec="C1",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
        description="test_description",
    )


def test_engine_update_subset_parameters1(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test engine create standard workflow with a subset of parameters:
     (new_engine_name, description, scale, warmup)
    """

    engine_mock = generic_engine_update(
        configure_resource_manager,
        "--name engine_name --new_engine_name name_of_the_new_engine "
        "--description test_description --scale 42 --warmup ind",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec=None,
        description="test_description",
        engine_type=None,
        scale=42,
        auto_stop=None,
        warmup=WarmupMethod.PRELOAD_INDEXES,
    )


def test_engine_update_subset_parameters2(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test engine create standard workflow with a subset of parameters:
     (spec, type, auto_stop)
    """

    engine_mock = generic_engine_update(
        configure_resource_manager,
        "--name engine_name --spec i3.xlarge --type ro --auto_stop 8393",
    )

    engine_mock.update.assert_called_once_with(
        name=None,
        spec="i3.xlarge",
        description=None,
        engine_type=EngineType.DATA_ANALYTICS,
        scale=None,
        auto_stop=8393,
        warmup=None,
    )


def test_engine_update_not_exists(configure_resource_manager: Sequence) -> None:
    """
    Test engine update, engine not exists
    """
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager
    engines_mock.get_by_name.side_effect = FireboltError("engine doesn't exist")

    result = CliRunner(mix_stderr=False).invoke(
        update,
        "--name engine_name --warmup all".split(),
    )

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")

    assert result.stdout == "", ""
    assert "engine doesn't exist" in result.stderr, ""
    assert result.exit_code != 0, ""


def test_engine_no_parameters_passed() -> None:
    """
    Test engine update, no parameters are passed for the update
    """

    result = CliRunner(mix_stderr=False).invoke(
        update,
        "--name engine_name".split(),
    )

    assert result.stdout == "", ""
    assert "Nothing to update" in result.stderr, ""
    assert result.exit_code != 0, ""
