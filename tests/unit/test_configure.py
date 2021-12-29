from configparser import ConfigParser
from os import path

from appdirs import user_config_dir
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.configure import config_file, config_section, configure


def validate_file_config(config):
    cp = ConfigParser()
    cp.read(config_file)

    cli_config = cp[config_section]
    assert set(list(cli_config.keys())) == set(config.keys()) and [
        cli_config[k] == config[k] for k in config.keys()
    ], "Invalid config written with configure command"


def test_configure_happy_path(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
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
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Created new config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "password": "password",
            "account_name": "account_name",
            "database_name": "database_name",
            "engine_name": "engine_name",
            "api_endpoint": "api_endpoint",
        }
    )

    fs.remove(config_file)

    # test some parameters missing, only -u and --engine-url
    result = runner.invoke(
        configure,
        [
            "-u",
            "username",
            "--engine-url",
            "engine_url",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Created new config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "engine_url": "engine_url",
        }
    )


def test_configure_prompt(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [],
        input="\n".join(
            ["username", "password", "account_name", "database_name", "engine_name"]
        ),
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Created new config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "password": "password",
            "account_name": "account_name",
            "database_name": "database_name",
            "engine_name": "engine_name",
        }
    )

    result = runner.invoke(
        configure,
        [],
        input="\n".join(
            ["username", "password", "account_name", "database_name", "", "engine_url"]
        ),
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Updated existing config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "password": "password",
            "account_name": "account_name",
            "database_name": "database_name",
            "engine_url": "engine_url",
        }
    )


def test_configure_overrides(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--username",
            "username",
            "--database-name",
            "database_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Created new config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "database_name": "database_name",
        }
    )

    result = runner.invoke(
        configure,
        [
            "--username",
            "username2",
            "--account-name",
            "account_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Updated existing config file" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username2",
            "database_name": "database_name",
            "account_name": "account_name",
        }
    )


def test_engine_name_url_together(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--engine-name",
            "engine_name",
            "--engine-url",
            "engine_url",
        ],
    )
    assert result.exit_code == 2, "invalid exit code for configure usage error"
    assert (
        "engine-name and engine-url are mutually exclusive options" in result.stdout
    ), "Invalid result message"

    # no config file created on error
    assert not path.exists(config_file)


def test_engine_name_url_overrides(fs: FakeFilesystem) -> None:
    fs.create_dir(user_config_dir())
    runner = CliRunner()
    result = runner.invoke(
        configure,
        [
            "--engine-name",
            "engine_name",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Created new config file" in result.stdout, "Invalid result message"

    validate_file_config({"engine_name": "engine_name"})

    # override name with url
    result = runner.invoke(
        configure,
        [
            "--engine-url",
            "engine_url",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Updated existing config file" in result.stdout, "Invalid result message"

    # name replaced with url
    validate_file_config({"engine_url": "engine_url"})
