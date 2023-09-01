import json
import time
from collections import namedtuple

import pytest
from click.testing import CliRunner

from firebolt_cli.main import main


def test_engine_list(engine_name: str, stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test engine list with and without filter
    """

    # Test without filter
    result = cli_runner.invoke(main, f"engine list --json".split())
    assert result.exit_code == 0, result.stderr
    assert result.stderr == ""

    output = json.loads(result.stdout)
    assert len(output) >= 2
    assert engine_name in {engine["name"] for engine in output}
    assert stopped_engine_name in {engine["name"] for engine in output}

    # Test with filter
    result = cli_runner.invoke(
        main, f"engine list --json --name-contains {stopped_engine_name}".split()
    )
    assert result.exit_code == 0, result.stderr
    assert result.stderr == ""

    output = json.loads(result.stdout)
    assert len(output) >= 1
    assert all([stopped_engine_name in engine["name"] for engine in output])


def test_engine_list_database(
    engine_name: str, stopped_engine_name: str, database_name: str, cli_runner: CliRunner
) -> None:
    """
    test engine list with filter by database
    """
    result = cli_runner.invoke(
        main, f"engine list --database {database_name} --json".split()
    )
    output = json.loads(result.stdout)

    assert len(output) == 2
    assert engine_name in {engine["name"] for engine in output}
    assert stopped_engine_name in {engine["name"] for engine in output}

    result = cli_runner.invoke(
        main,
        f"engine list --database {database_name} --json "
        f"--name-contains {stopped_engine_name}".split(),
    )
    output = json.loads(result.stdout)

    assert len(output) == 1
    assert output[0]["name"] == stopped_engine_name


def test_engine_start_running(engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test start engine, which is running should not fail
    """
    result = cli_runner.invoke(main, f"engine start {engine_name}".split())

    assert result.stderr == ""
    assert result.exit_code == 0, result.stderr


def test_engine_stop_stopped(stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test stop engine, which is stopped should not fail
    """

    result = cli_runner.invoke(main, f"engine stop {stopped_engine_name}".split())

    assert result.stderr == ""
    assert result.exit_code == 0, result.stderr


@pytest.mark.skip
@pytest.mark.slow
def test_engine_start_stop(stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test engine start/stop happy path
    """

    result = cli_runner.invoke(
        main, f"engine start {stopped_engine_name}".split()
    )
    assert result.exit_code == 0, result.stderr

    result = cli_runner.invoke(main, f"engine status {stopped_engine_name}".split())
    assert result.exit_code == 0, result.stderr
    assert "running" in result.stdout.lower()

    result = cli_runner.invoke(
        main, f"engine stop {stopped_engine_name}".split()
    )
    assert result.exit_code == 0, result.stderr

    result = cli_runner.invoke(main, f"engine status {stopped_engine_name}".split())
    assert result.exit_code == 0, result.stderr
    assert "stopped" in result.stdout.lower()


def test_engine_status(engine_name: str, stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Check status of running engine is running
    Check status of stopped engine is stopped
    Check status of non-existing engine returns an error
    """
    runner = cli_runner

    result = runner.invoke(main, f"engine status {engine_name}".split())
    assert result.exit_code == 0, result.stderr
    assert "running" in result.stdout.lower()

    result = runner.invoke(main, f"engine status {stopped_engine_name}".split())
    assert result.exit_code == 0, result.stderr
    assert "stopped" in result.stdout.lower()

    result = runner.invoke(
        main, f"engine status {engine_name}_non_existing_engine".split()
    )
    assert result.exit_code != 0
    assert result.stderr != ""


def test_engine_update_single_parameter(database_name: str, cli_runner: CliRunner) -> None:
    """
    Test updating single parameter one by one
    """
    runner = cli_runner

    engine_name = f"cli_integration_test_engine{int(time.time())}"
    result = runner.invoke(
        main,
        f"engine create --name {engine_name} --spec S1 "
        f"--database-name {database_name}".split(),
    )
    assert result.exit_code == 0, result.stderr

    _ParamValue = namedtuple("ParamValue", "set expected output_name")
    ENGINE_UPDATE_PARAMS = {
        "scale": _ParamValue(2, 2, "scale"),
        "spec": _ParamValue("S1", "S1", "instance_type"),
        "auto-stop": _ParamValue("1233", "20:33:00", "auto_stop"),
        "warmup": _ParamValue("all", "PRELOAD_ALL_DATA", "warm_up"),
    }

    for param, value in ENGINE_UPDATE_PARAMS.items():

        result = runner.invoke(
            main,
            f"engine update --name {engine_name} --{param} {value.set} --json".split(),
        )
        assert result.exit_code == 0, result.stderr
        output = json.loads(result.stdout)

        assert output[value.output_name] == value.expected

    runner.invoke(main, f"engine drop {engine_name} --yes")
    assert result.exit_code == 0, result.stderr


def test_engine_update_auto_stop(stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    test engine update --auto_stop, set to zero means it is always on
    """
    runner = cli_runner

    result = runner.invoke(
        main,
        f"engine update --name {stopped_engine_name} --auto-stop 0".split(),
    )
    assert result.exit_code == 0, result.stderr
    assert "ALWAYS ON" in result.stdout

    result = runner.invoke(
        main,
        f"engine update --name {stopped_engine_name} --auto-stop 313".split(),
    )
    assert result.exit_code == 0, result.stderr
    assert "5:13:00" in result.stdout


@pytest.mark.slow
def test_engine_restart_stopped(
    stopped_engine_name: str, cli_runner: CliRunner
) -> None:
    """
    Test restart engine, which is stopped should succeed
    """
    result = cli_runner.invoke(main, f"engine restart {stopped_engine_name}".split(), catch_exceptions=False)

    assert result.stderr == ""
    assert result.exit_code == 0, result.stderr

    # Check that engine actually running after restart
    result = cli_runner.invoke(
        main, f"engine status {stopped_engine_name}".split()
    )

    assert result.exit_code == 0, result.stderr
    assert "running" in result.stdout.lower()

    result = cli_runner.invoke(
        main, f"engine stop {stopped_engine_name}".split()
    )

    assert result.exit_code == 0, result.stderr
    

@pytest.mark.slow
def test_engine_restart_running(engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test restart engine, which is running should
    restart an engine and wait until it is running
    """
    result = cli_runner.invoke(main, f"engine restart {engine_name}".split())

    assert result.stderr == ""
    assert result.exit_code == 0, result.stderr

    # Check that engine actually running after restart
    result = cli_runner.invoke(
        main, f"engine status {engine_name}".split()
    )
    assert result.exit_code == 0, result.stderr
    assert "running" in result.stdout.lower()


def test_engine_create_minimal(
    engine_name: str, database_name: str, default_region: str, cli_runner: CliRunner
):
    """
    test engine create/drop with minimum amount of parameters
    """
    result = cli_runner.invoke(
        main,
        f"engine get-instance-types --json ".split(),
    )
    assert result.exit_code == 0, result.stderr
    instance_list = json.loads(result.stdout)
    instance_spec = instance_list[0]["name"]

    engine_name = f"{engine_name}_test"

    result = cli_runner.invoke(
        main,
        f"engine create --json "
        f"--name {engine_name} "
        f"--database-name {database_name} "
        f"--spec {instance_spec}".split(),
    )
    assert result.exit_code == 0, result.stderr
    create_output = json.loads(result.stdout)
    assert create_output["name"] == engine_name
    assert create_output["attached_to_database"] == database_name

    result = cli_runner.invoke(
        main,
        f"engine describe --json {engine_name} ".split(),
    )
    assert result.exit_code == 0, result.stderr
    describe_output = json.loads(result.stdout)
    assert describe_output == create_output

    result = cli_runner.invoke(
        main, f"engine drop {engine_name} --yes".split()
    )
    assert result.exit_code == 0, result.stderr


def test_engine_create_existing(engine_name: str, database_name: str, cli_runner: CliRunner):
    """
    Test engine create, if the name of engine is already taken
    """
    result = cli_runner.invoke(
        main,
        f"engine create --json "
        f"--name {engine_name} "
        f"--database-name {database_name} "
        f" --spec B1 ".split(),
    )
    assert "already exists" in result.stderr
    assert result.exit_code != 0


def test_engine_drop_not_existing(engine_name: str, cli_runner: CliRunner):
    """
    engine drop non-existing engine should return an error
    """
    result = cli_runner.invoke(
        main, f"engine drop {engine_name}_not_existing_db --yes".split()
    )
    assert result.exit_code != 0
    assert "not found" in result.stderr.lower()
