from configparser import ConfigParser

from appdirs import user_config_dir
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.configure import configure
from firebolt_cli.main import main
from firebolt_cli.utils import config_file, config_section, read_config


def validate_file_config(config):
    cp = ConfigParser(interpolation=None)
    cp.read(config_file)

    cli_config = cp[config_section]

    assert set(config.keys()).issubset(set(cli_config.keys())) and [
        cli_config[k] == config[k] for k in config.keys()
    ], "Invalid config written with configure command"


def test_configure_happy_path(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())

    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--client-id",
            "client_id",
            "--client-secret",
            "client_secret",
            "--account-name",
            "account_name",
            "--database-name",
            "database_name",
            "--engine-name",
            "engine_name",
            "--api-endpoint",
            "api_endpoint",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "account_name": "account_name",
            "database_name": "database_name",
            "engine_name": "engine_name",
            "api_endpoint": "api_endpoint",
        }
    )

    fs.remove(config_file)

    # test some parameters missing, only -u and --engine-name
    result = runner.invoke(
        configure,
        [
            "-c",
            "client_id",
            "--engine-name",
            "engine_url.firebolt.io",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "client_id": "client_id",
            "engine_name": "engine_url",
        }
    )


def test_configure_prompt(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [],
        input="\n".join(
            ["client_id", "client_secret", "account_name", "database_name", "engine_name"]
        ),
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "client_id": "client_id",
        "client_secret": "client_secret",
        "account_name": "account_name",
        "database_name": "database_name",
        "engine_name": "engine_name",
    }


def test_configure_overrides(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--client-id",
            "client_id",
            "--database-name",
            "database_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "client_id": "client_id",
        "database_name": "database_name",
    }

    result = runner.invoke(
        configure,
        [
            "--client-id",
            "client_id2",
            "--account-name",
            "account_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "client_id": "client_id2",
        "database_name": "database_name",
        "account_name": "account_name",
    }


def test_configure_short_version(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        main,
        "config --client-id client_id".split(),
    )

    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "client_id": "client_id",
    }


def test_configure_reset(fs: FakeFilesystem) -> None:
    """ """

    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--client-id",
            "client_id",
            "--database-name",
            "database_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "client_id": "client_id",
        "database_name": "database_name",
    }

    result = runner.invoke(
        configure,
        ["reset"],
    )

    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {}
