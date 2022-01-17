import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.configure import configure


@pytest.fixture()
def configure_cli(fs: FakeFilesystem) -> None:
    def inner_configure_cli():
        fs.create_dir(user_config_dir())
        runner = CliRunner()
        result = runner.invoke(
            configure,
            [
                "--username",
                "username",
                "--account-name",
                "account_name",
                "--engine-name",
                "engine_name",
                "--api-endpoint",
                "api_endpoint",
                "--database-name",
                "default",
            ],
            input="password",
        )

        assert result.exit_code == 0, "configuration of cli failed"

    return inner_configure_cli
