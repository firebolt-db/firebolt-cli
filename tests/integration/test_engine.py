import json
import time
from collections import namedtuple

from click.testing import CliRunner

from firebolt_cli.main import main


def test_engine_start_running(engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test start engine, which is running should fail
    """
    result = cli_runner.invoke(main, f"engine start --name {engine_name}".split())

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_stop_stopped(stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test stop engine, which is stopped should fail
    """

    result = cli_runner.invoke(
        main, f"engine stop --name {stopped_engine_name}".split()
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_start_stop(stopped_engine_name: str, cli_runner: CliRunner) -> None:
    """
    Test engine start/stop happy path
    """

    result = cli_runner.invoke(
        main, f"engine start --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main, f"engine status --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0
    assert "running" in result.stdout.lower()

    result = cli_runner.invoke(
        main, f"engine stop --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main, f"engine status --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0
    assert "stopped" in result.stdout.lower()


def test_engine_status(engine_name: str, stopped_engine_name: str) -> None:
    """
    Check status of running engine is running
    Check status of stopped engine is stopped
    Check status of non-existing engine returns an error
    """
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(main, f"engine status --name {engine_name}".split())
    assert result.exit_code == 0
    assert "running" in result.stdout.lower()

    result = runner.invoke(main, f"engine status --name {stopped_engine_name}".split())
    assert result.exit_code == 0
    assert "stopped" in result.stdout.lower()

    result = runner.invoke(
        main, f"engine status --name {engine_name}_non_existing_engine".split()
    )
    assert result.exit_code != 0
    assert result.stderr != ""


def test_engine_update_subset(database_name: str) -> None:
    runner = CliRunner(mix_stderr=False)

    engine_name = f"cli_integration_test_engine{int(time.time())}"
    result = runner.invoke(
        main,
        f"engine create --name {engine_name} --spec i3.2xlarge "
        f"--database-name {database_name} --region us-east-1".split(),
    )
    assert result.exit_code == 0

    ParamValue = namedtuple("ParamValue", "set expected")
    ENGINE_UPDATE_PARAMS = {
        "type": ParamValue("ro", "ENGINE_SETTINGS_PRESET_DATA_ANALYTICS"),
        "scale": ParamValue(23, 23),
        "spec": ParamValue("i3.xlarge", "i3.xlarge"),
        "auto-stop": ParamValue("1233", "20:33:00"),
        "warmup": ParamValue("all", "ENGINE_SETTINGS_WARM_UP_ALL"),
        "description": ParamValue("new engine description", "new engine description"),
    }

    for param, value in ENGINE_UPDATE_PARAMS.items():

        result = runner.invoke(
            main,
            f"engine update --name {engine_name} --{param} {value.set} --json".split(),
        )
        assert result.exit_code == 0
        output_update = json.loads(result.stdout)

        result = runner.invoke(main, f"engine status --name {engine_name} --json")
        assert result.exit_code == 0
        output_status = json.loads(result.stdout)

        assert output_update == output_status, "output of update is different to status"
        assert output_status[param] == value.expected
