import json
from collections import namedtuple
from typing import Callable, Optional, Sequence
from unittest import mock
from unittest.mock import ANY

import pytest
from click.testing import CliRunner, Result
from firebolt.common.exception import FireboltError
from firebolt.service.types import (
    EngineStatus,
    EngineType,
    WarmupMethod,
)
from pytest_mock import MockerFixture

from firebolt_cli.engine import (
    create,
    describe,
    drop,
    get_instance_types,
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
    rm, _, _ = configure_resource_manager

    rm.engines.get.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(start, ["not_existing_engine"])

    rm.engines.get.assert_called_once_with("not_existing_engine")

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def engine_start_stop_generic(
    command: Callable,
    configure_resource_manager: Sequence,
    state_before_call: EngineStatus,
    state_after_call: EngineStatus,
    check_engine_start_call: bool = False,
    check_engine_restart_call: bool = False,
    check_engine_stop_call: bool = False,
) -> Result:
    """
    generic start/stop engine procedure check
    """
    rm, _, engine_mock = configure_resource_manager

    rm.engines.get.return_value = engine_mock
    engine_mock.current_status = state_before_call

    engine_mock_after_call = mock.MagicMock()

    engine_mock_after_call.current_status = state_after_call

    engine_mock.start.return_value = engine_mock_after_call
    engine_mock.stop.return_value = engine_mock_after_call
    engine_mock_after_call.start.return_value = engine_mock_after_call
    engine_mock_after_call.stop.return_value = engine_mock_after_call    


    result = CliRunner(mix_stderr=False).invoke(
        command,
        ["engine_name"]
    )

    rm.engines.get.assert_called_once_with("engine_name")

    if check_engine_start_call:
        engine_mock.start.assert_called_once()
    if check_engine_stop_call:
        engine_mock.stop.assert_called_once()
    if check_engine_restart_call:
        engine_mock.stop.assert_called_once()
        engine_mock_after_call.start.assert_called_once()

    return result


def test_engine_start_failed(configure_resource_manager: Sequence) -> None:
    """
    Engine was in stopped state before starting,
    but didn't change the state to running after the start call
    """

    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatus.STOPPED,
        state_after_call=EngineStatus.FAILED,
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
        state_before_call=EngineStatus.STOPPED,
        state_after_call=EngineStatus.RUNNING,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_from_failed(configure_resource_manager: Sequence) -> None:
    """
    Engine was in failed state before starting, start is not possible,
    suggest the user to restart the engine
    """
    rm, _, engine_mock = configure_resource_manager

    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatus.FAILED,
        state_after_call=EngineStatus.FAILED,
        check_engine_stop_call=False,
    )

    engine_mock.start.assert_not_called()

    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_wrong_state(configure_resource_manager: Sequence) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    result = engine_start_stop_generic(
        start,
        configure_resource_manager,
        state_before_call=EngineStatus.STARTING,
        state_after_call=EngineStatus.STARTING,
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
        state_before_call=EngineStatus.RUNNING,
        state_after_call=EngineStatus.FAILED,
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
        state_before_call=EngineStatus.RUNNING,
        state_after_call=EngineStatus.STOPPED,
        check_engine_stop_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_status(configure_resource_manager: Sequence) -> None:
    (
        rm,
        database_mock,
        engine_mock,
    ) = configure_resource_manager

    engine_mock.current_status.name = "engine running"

    result = CliRunner(mix_stderr=False).invoke(status, ["engine_name"])

    rm.engines.get.assert_called_once_with("engine_name")

    assert "engine running" in result.stdout
    assert result.stderr == ""
    assert result.exit_code == 0


def test_engine_create_happy_path(configure_resource_manager: Sequence) -> None:
    """
    Test engine create standard workflow
    """
    rm, _, engine_mock = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        create,
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
    )

    rm.databases.get.assert_called_once()
    rm.engines.create.assert_called_once()

    engine_mock.attach_to_database.assert_called_once()

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""


def test_engine_create_database_not_found(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if the database it is attached to doesn't exist
    """
    rm, _, rm.engines = configure_resource_manager

    rm.databases.get.side_effect = FireboltError("database not found")

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

    rm.databases.get.assert_called_once()
    rm.engines.create.assert_not_called()

    rm.databases.attach_to_engine.assert_not_called()

    assert result.stderr != "", ""
    assert result.exit_code != 0, ""


def test_engine_create_name_taken(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if the engine name is already taken
    """
    rm, _, _ = configure_resource_manager
    rm.engines.create.side_effect = FireboltError("engine name already exists")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
    )

    rm.databases.get.assert_called_once()
    rm.engines.create.assert_called_once()

    rm.databases.attach_to_engine.assert_not_called()

    assert result.stderr != "", ""
    assert result.exit_code != 0, ""


def test_engine_create_binding_failed(configure_resource_manager: Sequence) -> None:
    """
    Test creation of engine if for some reason binding failed;
    Check, that the database deletion was requested
    """
    (
        rm,
        database_mock,
        engine_mock,
    ) = configure_resource_manager

    engine_mock.attach_to_database.side_effect = FireboltError("binding failed")

    result = CliRunner(mix_stderr=False).invoke(
        create,
        ["--name", "engine_name", "--database-name", "database_name", "--spec", "C1"],
    )

    rm.databases.get.assert_called_once()
    rm.engines.create.assert_called_once()

    engine_mock.delete.assert_called_once()
    rm.databases.attach_to_engine.assert_not_called()

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
        database_mock,
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

    assert result.exit_code == 0, result.stderr

    rm.databases.get.assert_called_once_with("database_name")
    rm.engines.create.assert_called_once_with(
        name="engine_name",
        spec="C1",
        region="mock_region",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
    )

    engine_mock.attach_to_database.assert_called_once_with(database_mock)

    assert result.stdout != "", ""
    assert result.stderr == "", ""
    assert result.exit_code == 0, ""


def test_engine_status_not_found(configure_resource_manager: Sequence) -> None:
    rm, _, _ = configure_resource_manager

    rm.engines.get.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(status, ["non_existing_engine"])

    rm.engines.get.assert_called_once_with("non_existing_engine")

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_restart(configure_resource_manager: Sequence) -> None:
    """
    Test restart engine happy path
    """
    result = engine_start_stop_generic(
        restart,
        configure_resource_manager,
        state_before_call=EngineStatus.FAILED,
        state_after_call=EngineStatus.RUNNING,
        check_engine_restart_call=False,
    )

    assert result.exit_code == 0, result.stderr


def test_engine_restart_failed(configure_resource_manager: Sequence) -> None:
    """
    Test restart engine failed
    """
    result = engine_start_stop_generic(
        restart,
        configure_resource_manager,
        state_before_call=EngineStatus.FAILED,
        state_after_call=EngineStatus.FAILED,
        check_engine_restart_call=True,
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_restart_not_exist(configure_resource_manager: Sequence) -> None:
    """
    Test engine restart, if engine doesn't exist
    """
    rm, _, _ = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(restart, ["non_existing_engine"])

    rm.engines.get.assert_called_once_with("non_existing_engine")

    assert result.stderr != ""
    assert result.exit_code != 0


@pytest.mark.parametrize("list_command", ["list", "ls"])
def test_engine_list(configure_resource_manager: Sequence, list_command: str) -> None:
    """
    test engine list happy path
    """
    rm, _, _ = configure_resource_manager

    engine_mock1 = mock.MagicMock()
    engine_mock1.name = "engine_mock1"
    engine_mock1.current_status = (
        EngineStatus.RUNNING
    )
    engine_mock1.region = "mock_region"

    engine_mock2 = mock.MagicMock()
    engine_mock2.name = "engine_mock2"
    engine_mock2.current_status = (
        EngineStatus.STOPPED
    )
    engine_mock2.region = "mock_region"

    rm.engines.get_many.return_value = [engine_mock1, engine_mock2]

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine {list_command} --name-contains engine_name --json".split()
    )

    assert result.stderr == ""
    assert result.exit_code == 0

    output = json.loads(result.stdout)

    assert len(output) == 2
    assert output[0]["name"] == "engine_mock1"
    assert output[1]["name"] == "engine_mock2"

    rm.engines.get_many.assert_called_once_with(
        current_status_eq=None,
        current_status_not_eq=None,
        database_name=None,
        name_contains="engine_name",
        region_eq=None
    )


def test_engine_list_filter(configure_resource_manager: Sequence) -> None:
    """
    test engine list with --database and --name-contains
    """
    rm, _, _ = configure_resource_manager

    engine_mock1 = mock.MagicMock()
    engine_mock1.name = "engine_mock1"
    engine_mock1.current_status = (
        EngineStatus.RUNNING
    )
    engine_mock1.region = "mock_region"

    engine_mock2 = mock.MagicMock()
    engine_mock2.name = "engine_mock2"
    engine_mock2.current_status = (
        EngineStatus.STOPPED
    )
    engine_mock2.region = "mock_region"

    rm.engines.get_many.return_value = [
        engine_mock1,
        engine_mock2,
    ]

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine list --database db_name --name-contains mock1 --current-status running --current-status-not stopped --region us-east-1 --json".split()
    )
    assert result.stderr == ""
    assert result.exit_code == 0

    output = json.loads(result.stdout)

    assert len(output) == 2
    assert output[0]["name"] == "engine_mock1"
    assert output[1]["name"] == "engine_mock2"

    rm.engines.get_many.assert_called_once_with(
        current_status_eq="Running",
        current_status_not_eq="Stopped",
        database_name="db_name",
        name_contains="mock1",
        region_eq="us-east-1"
    )


def generic_engine_update(configure_resource_manager: Sequence, parameters: str):
    """
    Test engine create standard workflow with all optional parameters
    """
    (
        rm,
        database_mock,
        engine_mock,
    ) = configure_resource_manager
    engine_mock.update.return_value = engine_mock

    result = CliRunner(mix_stderr=False).invoke(update, parameters.split())

    assert result.stderr == "", ""
    assert result.exit_code == 0, ""
    assert result.stdout != "", ""

    rm.engines.get.assert_called_once_with("engine_name")

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
        "--spec C1 --type rw --scale 23 --auto-stop 893 --warmup all",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec="C1",
        engine_type=EngineType.GENERAL_PURPOSE,
        scale=23,
        auto_stop=893,
        warmup=WarmupMethod.PRELOAD_ALL_DATA,
    )


def test_engine_update_subset_parameters1(
    configure_resource_manager: Sequence,
) -> None:
    """
    Test engine create standard workflow with a subset of parameters:
     (new_engine_name, scale, warmup)
    """

    engine_mock = generic_engine_update(
        configure_resource_manager,
        "--name engine_name --new-engine-name name_of_the_new_engine "
        "--scale 42 --warmup ind",
    )

    engine_mock.update.assert_called_once_with(
        name="name_of_the_new_engine",
        spec=None,
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
        "--name engine_name --spec i3.xlarge --type ro --auto-stop 8393",
    )

    engine_mock.update.assert_called_once_with(
        name=None,
        spec="i3.xlarge",
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
        database_mock,
        engine_mock,
    ) = configure_resource_manager
    rm.engines.get.side_effect = FireboltError("engine doesn't exist")

    result = CliRunner(mix_stderr=False).invoke(
        update,
        "--name engine_name --warmup all".split(),
    )

    rm.engines.get.assert_called_once_with("engine_name")

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

    rm, _, engine_mock = configure_resource_manager

    result = CliRunner(mix_stderr=False).invoke(
        drop,
        ["to_drop_engine_name"] + additional_parameters,
        input=input,
    )

    rm.engines.get.assert_called_once_with("to_drop_engine_name")
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
    rm, _, _ = configure_resource_manager

    rm.engines.get.side_effect = RuntimeError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(drop, "to_drop_engine_name".split())

    rm.engines.get.assert_called_once_with("to_drop_engine_name")

    assert result.stderr != "", "cli should fail with an error message in stderr"
    assert result.exit_code != 0, "non-zero exit code"


def test_engine_describe_json(configure_resource_manager: Sequence) -> None:
    """ """
    rm, database_mock, engine_mock = configure_resource_manager

    engine_mock.name = "to_describe_engine"
    engine_mock.current_status = (
        EngineStatus.RUNNING
    )
    engine_mock.auto_stop = 4800
    engine_mock.type = EngineType.DATA_ANALYTICS
    engine_mock.warmup = WarmupMethod.PRELOAD_INDEXES
    engine_mock._database_name = database_mock.name
    engine_mock.spec = mock.MagicMock()
    engine_mock.spec.name = "B2"
    engine_mock.scale = 2

    result = CliRunner(mix_stderr=False).invoke(
        describe, ["to_describe_engine", "--json"]
    )

    assert result.stderr == ""
    assert result.exit_code == 0

    engine_description = json.loads(result.stdout)

    for param in [
        "name",
        "status",
        "auto_stop",
        "type",
        "warm_up",
        "attached_to_database",
        "instance_type",
        "scale",
    ]:
        assert param in engine_description

    assert engine_description["name"] == "to_describe_engine"
    assert engine_description["status"] == "RUNNING"
    assert engine_description["auto_stop"] == "1:20:00"
    assert engine_description["type"] == "Data Analytics"
    assert engine_description["warm_up"] == "Preload Indexes"
    assert engine_description["attached_to_database"] == database_mock.name
    assert engine_description["instance_type"] == "B2"
    assert engine_description["scale"] == 2


def test_engine_describe_not_found(configure_resource_manager: Sequence) -> None:
    """ """
    rm, _, _ = configure_resource_manager
    rm.engines.get.side_effect = FireboltError("engine not found")

    result = CliRunner(mix_stderr=False).invoke(describe, ["to_describe_engine"])

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_get_instance_types(configure_resource_manager: Sequence) -> None:
    """
    Happy path of getting a list of instance types
    """

    rm, _, _ = configure_resource_manager

    _InstanceType = namedtuple(
        "InstanceType",
        "name, cpu_virtual_cores_count, memory_size_bytes, storage_size_bytes, price_per_hour_cents",
    )
    rm.instance_types.instance_types = [
        _InstanceType("B1", 2, 123, 321, 10)
    ]

    result = CliRunner(mix_stderr=False).invoke(
        get_instance_types, ["--json"],
    )

    assert result.stderr == ""
    assert result.exit_code == 0

    output = json.loads(result.stdout)
    assert len(output) == 1

