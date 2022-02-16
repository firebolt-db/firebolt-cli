from configparser import ConfigParser

from appdirs import user_config_dir
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.configure import configure
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
    test_password = "pasword523%@!$$@#%@#!"
    fs.create_file("pswd", contents=test_password)

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
            "--password",
        ],
        input=test_password,
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
            "account_name": "account_name",
            "database_name": "database_name",
            "engine_name": "engine_name",
            "api_endpoint": "api_endpoint",
        }
    )

    assert read_config().get("password") == test_password

    fs.remove(config_file)

    # test some parameters missing, only -u and --engine-name
    result = runner.invoke(
        configure,
        [
            "-u",
            "username",
            "--engine-name",
            "engine_url.firebolt.io",
        ],
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    validate_file_config(
        {
            "username": "username",
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
            ["username", "password", "account_name", "database_name", "engine_name"]
        ),
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "username": "username",
        "account_name": "account_name",
        "database_name": "database_name",
        "engine_name": "engine_name",
        "password": "password",
    }

    result = runner.invoke(
        configure,
        [],
        input="\n".join(
            [
                "username",
                "password",
                "account_name",
                "database_name",
                "engine_url.firebolt.io",
            ]
        ),
    )
    assert result.exit_code == 0, "non-zero exit code for configure"
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "username": "username",
        "account_name": "account_name",
        "database_name": "database_name",
        "engine_name": "engine_url.firebolt.io",
        "password": "password",
    }


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
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "username": "username",
        "database_name": "database_name",
    }

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
    assert "Successfully" in result.stdout, "Invalid result message"

    assert read_config() == {
        "username": "username2",
        "database_name": "database_name",
        "account_name": "account_name",
    }
