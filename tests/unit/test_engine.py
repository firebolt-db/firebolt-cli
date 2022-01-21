from unittest import mock

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner, Result
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from firebolt.service.types import EngineStatusSummary
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.configure import configure
from firebolt_cli.engine import start


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
        start,
        ["--name", "not_existing_engine"],
    )

    rm.assert_called_once()
    engine_mock.get_by_name.assert_called_once_with("not_existing_engine")

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def engine_start_generic(
    mocker: MockerFixture,
    state_before_starting: EngineStatusSummary,
    state_after_starting: EngineStatusSummary,
    nowait: bool,
    check_engine_start_call: bool,
) -> Result:
    """
    generic start engine procedure check
    """
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    engines_mock = mocker.patch.object(ResourceManager, "engines", create=True)

    engine_mock_before_start = mock.MagicMock()
    engine_mock_before_start.current_status_summary = (
        state_before_starting  # EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
    )
    engines_mock.get_by_name.return_value = engine_mock_before_start

    engine_mock_after_start = mock.MagicMock()
    engine_mock_after_start.current_status_summary = (
        state_after_starting  # EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED
    )
    engine_mock_before_start.start.return_value = engine_mock_after_start

    additional_parameters = ["--nowait"] if nowait else []

    result = CliRunner(mix_stderr=False).invoke(
        start,
        ["--name", "broken_engine"] + additional_parameters,
    )

    rm.assert_called_once()
    engines_mock.get_by_name.assert_called_once_with("broken_engine")

    if check_engine_start_call:
        engine_mock_before_start.start.assert_called_once_with(
            wait_for_startup=not nowait
        )

    return result


def test_engine_start_failed(mocker: MockerFixture) -> None:
    """
    Engine was in stopped state before starting,
    but didn't change the state to running after the start call
    """

    result = engine_start_generic(
        mocker,
        state_before_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"


def test_engine_start_happy_path(mocker: MockerFixture) -> None:
    """
    Test the normal behavior
    """

    result = engine_start_generic(
        mocker,
        state_before_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
        nowait=False,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_happy_path_nowait(mocker: MockerFixture) -> None:
    """
    Test normal behavior with nowait parameter
    """
    result = engine_start_generic(
        mocker,
        state_before_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        state_after_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=True,
    )

    assert result.exit_code == 0, "cli was expected to execute correctly, but it failed"


def test_engine_start_wrong_state(mocker: MockerFixture) -> None:
    """
    Name of a non-existing engine is provided to the start engine command
    """
    result = engine_start_generic(
        mocker,
        state_before_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        state_after_starting=EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        nowait=True,
        check_engine_start_call=False,
    )

    assert result.stderr != "", "cli should fail, but stderr is empty"
    assert result.exit_code != 0, "cli was expected to fail, but it didn't"
