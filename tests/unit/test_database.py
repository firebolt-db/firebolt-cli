from unittest import mock

from click.testing import CliRunner
from firebolt.service.manager import ResourceManager
from pytest_mock import MockerFixture

from firebolt_cli.database import create


def test_database_create(mocker: MockerFixture) -> None:

    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    databases_mock = mocker.patch.object(ResourceManager, "databases", create=True)

    result = CliRunner().invoke(
        create,
        [
            "--database-name",
            "test_database",
            "--username",
            "username",
            "--account-name",
            "account_name",
            "--api-endpoint",
            "api_endpoint",
            "--password",
        ],
        input="password",
    )

    rm.assert_called()
    databases_mock.create.assert_called_with(name="test_database", region=mock.ANY)
    assert result.exit_code == 0, "non-zero exit code"
