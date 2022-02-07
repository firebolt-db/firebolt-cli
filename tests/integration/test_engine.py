import json

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


def test_engine_create_minimal(engine_name: str, database_name: str):
    """
    test engine create/drop with minimum amount of parameters
    """
    engine_name = f"{engine_name}_test_engine_create_minimal"

    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"engine create --json "
        f"--name {engine_name} "
        f"--database_name {database_name} "
        f" --spec i3.large "
        f"--region us-east-1".split(),
    )
    assert result.exit_code == 0
    output = json.loads(result.stdout)
    assert output["name"] == engine_name
    assert output["attached_to_database"] == database_name

    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine drop --name {engine_name} --yes".split()
    )
    assert result.exit_code == 0


def test_engine_create_existing(engine_name: str, database_name: str):
    """
    Test engine create, if the name of engine is already taken
    """
    result = CliRunner(mix_stderr=False).invoke(
        main,
        f"engine create --json "
        f"--name {engine_name} "
        f"--database_name {database_name} "
        f" --spec i3.large "
        f"--region us-east-1".split(),
    )
    assert "not unique" in result.stderr
    assert result.exit_code != 0


def test_engine_drop_not_existing(engine_name: str):
    """
    engine drop non-existing engine should return an error
    """
    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine drop --name {engine_name}_not_existing_db --yes".split()
    )
    assert result.exit_code != 0
    assert "not found" in result.stderr.lower()


def test_engine_list(engine_name: str, stopped_engine_name: str) -> None:
    """
    Test engine list with and without filter
    """

    # Test without filter
    result = CliRunner(mix_stderr=False).invoke(main, f"engine list --json".split())
    assert result.exit_code == 0
    assert result.stderr == ""

    output = json.loads(result.stdout)
    assert len(output) >= 2
    assert engine_name in {engine["name"] for engine in output}
    assert stopped_engine_name in {engine["name"] for engine in output}

    # Test with filter
    result = CliRunner(mix_stderr=False).invoke(
        main, f"engine list --json --name-contains {stopped_engine_name}".split()
    )
    assert result.exit_code == 0
    assert result.stderr == ""

    output = json.loads(result.stdout)
    assert len(output) >= 1
    assert all([stopped_engine_name in engine["name"] for engine in output])
