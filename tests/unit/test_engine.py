import json
from typing import Callable, Optional, Sequence
from unittest import mock
from unittest.mock import ANY

import pytest
from click.testing import CliRunner, Result
from firebolt.common.exception import FireboltError
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)
from pytest_mock import MockerFixture

from firebolt_cli.engine import (
    create,
    describe,
    drop,
    restart,
    start,
    status,
    stop,
    update,
)
from firebolt_cli.main import main


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

    result = CliRunner(mix_stderr=False).invoke(start, ["not_existing_engine"])

    engines_mock.get_by_name.assert_called_once_with(name="not_existing_engine")

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def engine_start_stop_generic(
    command: Callable,
    configure_resource_manager: Sequence,
    state_before_call: EngineStatusSummary,
    state_after_call: EngineStatusSummary,
    wait: bool,
    check_engine_start_call: bool = False,
    check_engine_restart_call: bool = False,
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

    engine_mock.restart.return_value = engine_mock_after_call
    engine_mock.start.return_value = engine_mock_after_call
    engine_mock.stop.return_value = engine_mock_after_call

    additional_parameters = ["--wait"] if wait else ["--no-wait"]

    result = CliRunner(mix_stderr=False).invoke(
        command,
        additional_parameters + ["engine_name"],
    )

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")

    if check_engine_start_call:
        engine_mock.start.assert_called_once_with(wait_for_startup=wait)
    if check_engine_stop_call:
        engine_mock.stop.assert_called_once_with(wait_for_stop=wait)
    if check_engine_restart_call:
        engine_mock.restart.assert_called_once_with(wait_for_startup=wait)

    if check_engine_restart_call:
        engine_mock.restart.assert_called_once_with(wait_for_startup=wait)

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
        wait=True,
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
        wait=True,
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
        wait=False,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_from_failed(configure_resource_manager: Sequence) -> None:
    """
    Engine was in failed state before starting, start is not possible,
    suggest the user to restart the engine
    """
    rm, _, _, engines_mock, engine_mock = configure_resource_manager

    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        wait=True,
        check_engine_stop_call=False,
    )

    engine_mock.start.assert_not_called()

    assert "restart an engine first" in result.stderr != ""
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_wrong_state(configure_resource_manager: Sequence) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        wait=False,
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
        wait=True,
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
        wait=True,
        check_engine_stop_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_status(configure_resource_manager: Sequence) -> None:
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager

    engine_mock.current_status_summary.name = "engine running"

    result = CliRunner(mix_stderr=False).invoke(status, ["engine_name"])

    engines_mock.get_by_name.assert_called_once_with(name="engine_name")

    assert "engine running" in result.stdout
    assert result.stderr == ""
    assert result.exit_code == 0


def test_engine_status_default(
    mocker: MockerFixture, configure_resource_manager: Sequence
) -> None:
    (
        rm,
        databases_mock,
        database_mock,
        engines_mock,
        engine_mock,
    ) = configure_resource_manager

    engine_mock.current_status_summary.name = "engine running"

    get_default_database_engine_mock = mocker.patch(
        "firebolt_cli.engine.get_default_database_engine", return_value=engine_mock
    )

    result = CliRunner(mix_stderr=False).invoke(status, [])

    get_default_database_engine_mock.assert_called_once_with(ANY, "database_name")

    assert "engine running" in result.stdout
    assert result.stderr == ""
    assert result.exit_code == 0


def test_engine_create_happy_path(configure_resource_manager: Sequence) -> None:
    """
    Test engine create standard workflow
    """
    rm, databases_mock, database_mock, engines_mock, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        create,
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
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
            "--database-name",
            "database_name",
            "--spec",
            "C1",
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
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
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
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
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
            "--database-name",
            "database_name",
            "--spec",
            "C1",
            "--description",
            "test_description",
            "--type",
            "rw",
            "--scale",
            "23",
            "--auto-stop",
            "893",
            "--warmup",
            "all",
        ],
    )

    databases_mock.get_by_name.assert_called_once_with(name="database_name")
    engines_mock.create.assert_called_once_with(
        name="engine_name",
        spec="C1",
        region="us-east-1",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
        description="test_description",
        revision_spec_kwargs={"db_compute_instances_use_spot": False},
    )

    database_mock.attach_to_engine.assert_called_once_with(
        engine=engine_mock, is_default_engine=True
    )

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""


def test_engine_status_not_found(configure_resource_manager: Sequence) -> None:
    rm, _, _, engines_mock, _ = configure_resource_manager

    engines_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(status, ["non_existing_engine"])

    engines_mock.get_by_name.assert_called_once_with(name="non_existing_engine")
    rm.assert_called_once()

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_restart(configure_resource_manager: Sequence) -> None:
    """
    Test restart engine happy path
    """
    result = engine_start_stop_generic(
        restart,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        wait=True,
        check_engine_restart_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_restart_failed(configure_resource_manager: Sequence) -> None:
    """
    Test restart engine failed
    """
    result = engine_start_stop_generic(
        restart,
        configure_resource_manager,
        state_before_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        state_after_call=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        wait=True,
        check_engine_restart_call=True,
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_restart_not_exist(configure_resource_manager: Sequence) -> None:
    """
    Test engine restart, if engine doesn't exist
    """
    rm, _, _, engines_mock, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(restart, ["non_existing_engine"])

    engines_mock.get_by_name.assert_called_once_with(name="non_existing_engine")
    rm.assert_called_once()

    assert result.stderr != ""
    assert result.exit_code != 0


@pytest.mark.parametrize("list_command", ["list", "ls"])
def test_engine_list(configure_resource_manager: Sequence, list_command: str) -> None:
    """
    test engine list happy path
    """
    rm, _, _, engines_mock, _ = configure_resource_manager

    engine_mock1 = mock.MagicMock()
    engine_mock1.name = "engine_mock1"
    engine_mock1.current_status_summary = (
        EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
    )

    engine_mock2 = mock.MagicMock()
    engine_mock2.name = "engine_mock2"
    engine_mock2.current_status_summary = (
        EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
    )

    engines_mock.get_many.return_value = [engine_mock1, engine_mock2]

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine {list_command} --name-contains engine_name --json".split()
    )

    output = json.loads(result.stdout)

    assert len(output) == 2
    assert output[0]["name"] == "engine_mock1"
    assert output[1]["name"] == "engine_mock2"

    rm.assert_called_once()
    engines_mock.get_many.assert_called_once_with(
        name_contains="engine_name", order_by="ENGINE_ORDER_NAME_ASC"
    )

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
        "--name engine_name --new-engine-name name_of_the_new_engine "
        "--spec C1 --description test_description "
        "--type rw --scale 23 --auto-stop 893 --warmup all --use-spot",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec="C1",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
        description="test_description",
        use_spot=True,
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
        "--name engine_name --new-engine-name name_of_the_new_engine "
        "--description test_description --scale 42 --warmup ind",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec=None,
        description="test_description",
        engine_type=None,
        scale=42,
        auto_stop=None,
        use_spot=None,
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
        "--name engine_name --spec i3.xlarge --type ro --auto-stop 8393 --no-use-spot",
    )

    engine_mock.update.assert_called_once_with(
        name=None,
        spec="i3.xlarge",
        description=None,
        engine_type=EngineType.DATA_ANALYTICS,
        scale=None,
        auto_stop=8393,
        warmup=None,
        use_spot=False,
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


def engine_drop_generic_workflow(
    configure_resource_manager: Sequence,
    additional_parameters: Sequence[str],
    input: Optional[str],
    delete_should_be_called: bool,
) -> None:

    rm, _, _, engines_mock, engine_mock = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        drop,
        ["to_drop_engine_name"] + additional_parameters,
        input=input,
    )

    engines_mock.get_by_name.assert_called_once_with(name="to_drop_engine_name")
    if delete_should_be_called:
        engine_mock.delete.assert_called_once_with()

    assert result.exit_code == 0, "non-zero exit code"


def test_engine_drop(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing engine without confirmation prompt
    """
    engine_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=["--yes"],
        input=None,
        delete_should_be_called=True,
    )


def test_engine_drop_prompt_yes(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing database with confirmation prompt
    """
    engine_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=[],
        input="yes",
        delete_should_be_called=True,
    )


def test_engine_drop_prompt_no(configure_resource_manager: Sequence) -> None:
    """
    Happy path, deletion of existing database with confirmation prompt, and user rejects
    """
    engine_drop_generic_workflow(
        configure_resource_manager,
        additional_parameters=[],
        input="no",
        delete_should_be_called=False,
    )


def test_engine_drop_not_found(configure_resource_manager: Sequence) -> None:
    """
    Trying to drop the database, if the database is not found by name
    """
    rm, _, _, engines_mock, _ = configure_resource_manager

    engines_mock.get_by_name.side_effect = RuntimeError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(drop, "to_drop_engine_name".split())

    engines_mock.get_by_name.assert_called_once_with(name="to_drop_engine_name")

    assert result.stderr != "", "cli should fail with an error message in stderr"
    assert result.exit_code != 0, "non-zero exit code"


def test_engine_describe_json(configure_resource_manager: Sequence) -> None:
    """ """
    rm, _, database_mock, _, engine_mock = configure_resource_manager

    engine_mock.name = "to_describe_engine"
    engine_mock.description = "engine description"
    engine_mock.current_status_summary = (
        EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
    )
    engine_mock.latest_revision_key = None
    engine_mock.settings.preset = "ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE"
    engine_mock.settings.auto_stop_delay_duration = "4800s"
    engine_mock.settings.warm_up = "index"
    engine_mock.create_time = ""
    engine_mock.database = database_mock

    result = CliRunner(mix_stderr=False).invoke(
        describe, ["to_describe_engine", "--json"]
    )

    engine_description = json.loads(result.stdout)

    for param in [
        "name",
        "description",
        "auto_stop",
        "warm_up",
        "attached_to_database",
        "instance_type",
        "preset",
        "scale",
        "status",
    ]:
        assert param in engine_description

    assert engine_description["name"] == "to_describe_engine"
    assert engine_description["description"] == "engine description"
    assert engine_description["preset"] == "ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE"
    assert engine_description["warm_up"] == "index"
    assert engine_description["auto_stop"] == "1:20:00"
    assert engine_description["attached_to_database"] == database_mock.name
    assert engine_description["status"] == "ENGINE_STATUS_SUMMARY_RUNNING"

    assert result.stderr == ""
    assert result.exit_code == 0


def test_engine_describe_not_found(configure_resource_manager: Sequence) -> None:
    """ """
    rm, _, _, engines_mock, _ = configure_resource_manager
    engines_mock.get_by_name.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(describe, ["to_describe_engine"])

    assert result.stderr != ""
    assert result.exit_code != 0
