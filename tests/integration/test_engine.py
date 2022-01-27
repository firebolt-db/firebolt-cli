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

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine status --name {engine_name}".split()
    )
    assert result.exit_code == 0
    assert "running" in result.stdout.lower()

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine status --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0
    assert "stopped" in result.stdout.lower()

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine status --name {engine_name}_non_existing_engine".split()
    )
    assert result.exit_code != 0
    assert result.stderr != ""
