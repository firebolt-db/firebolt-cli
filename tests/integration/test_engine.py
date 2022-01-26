from click.testing import CliRunner

from firebolt_cli.main import main


def test_engine_start_running(engine_name: str):
    """
    Test start engine, which is running should fail
    """
    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine start --name {engine_name}".split()
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_stop_stopped(stopped_engine_name: str):
    """
    Test stop engine, which is stopped should fail
    """

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine stop --name {stopped_engine_name}".split()
    )

    assert result.stderr != ""
    assert result.exit_code != 0


def test_engine_start_stop(stopped_engine_name: str):
    """
    Test engine start/stop happy path
    """

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine start --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine stop --name {stopped_engine_name}".split()
    )
    assert result.exit_code == 0
