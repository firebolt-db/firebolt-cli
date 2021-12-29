from configparser import ConfigParser
from contextlib import contextmanager
from io import StringIO
from json import dumps, loads
from os import environ
from typing import Optional, Tuple
from unittest import mock

from click import command, echo
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.common_options import (
    _common_options,
    config_file,
    config_section,
)


@contextmanager
def create_config_file(fs: FakeFilesystem, config: dict) -> None:
    # make sure config will be flushed not to be reused in other tests
    with mock.patch("firebolt_cli.common_options._config", None):
        cp = ConfigParser()
        cp[config_section] = config
        content = StringIO()
        cp.write(content)

        fs.create_file(config_file, contents=content.getvalue())

        yield

        fs.remove(config_file)


def test_username_priority(fs: FakeFilesystem):
    """username is processed correctly, in correct proirity from different sources"""
    opt = _common_options[0]  # username option

    # helper command, dumps all options it received
    @command()
    @opt
    def test(**kwargs):
        echo(dumps(kwargs))

    def validate_command(command: Tuple, expected_value: str, err_msg: str):
        result = runner.invoke(*command)
        assert result.exit_code == 0, "non-zero exit code for "
        config = loads(result.output)
        assert "username" in config, "missing username command option"
        assert config["username"] == expected_value, err_msg

    with create_config_file(fs, {"username": "un_file"}):
        runner = CliRunner()

        with mock.patch.dict(environ, {"FIREBOLT_USERNAME": "un_env"}):
            # username is provided as option, env variable and in config file,
            # option should be chosen
            validate_command(
                (test, ["--username", "un_option"]),
                "un_option",
                "invalid username from option",
            )

            validate_command(
                (test, ["-u", "un_option"]),
                "un_option",
                "invalid username from option",
            )

            # username is provided as env variable and in config file,
            # env variable should be chosen
            validate_command(
                (test,),
                "un_env",
                "invalid username from env",
            )

        # username is provided in config file,
        # it should be read correctly
        validate_command(
            (test,),
            "un_file",
            "invalid username from file",
        )


def test_password_priority(fs: FakeFilesystem):
    """username is processed correctly, in correct proirity from different sources"""
    opt = _common_options[1]  # password option

    # helper command, dumps all options it received
    @command()
    @opt
    def test(**kwargs):
        echo(dumps(kwargs))

    def validate_command(
        command: Tuple, input: Optional[str], expected_value: str, err_msg: str
    ):
        result = runner.invoke(*command, input=input)
        if result.exit_code != 0:
            print(result.__dict__)
        assert result.exit_code == 0, "non-zero exit code for "
        prompt = "Password: \n"
        if result.output.startswith(prompt):
            config = loads(result.output[len(prompt) :])
        else:
            config = loads(result.output)
        assert "password" in config, "missing password command option"
        assert config["password"] == expected_value, err_msg

    with create_config_file(fs, {"password": "pw_file"}):
        runner = CliRunner()

        with mock.patch.dict(environ, {"FIREBOLT_PASSWORD": "pw_env"}):
            # username is provided as option, env variable and in config file,
            # option should be chosen
            validate_command(
                (test, ["--password"]),
                "pw_option",
                "pw_option",
                "invalid password from option",
            )

            validate_command(
                (test, ["-p"]),
                "pw_option",
                "pw_option",
                "invalid password from option",
            )

            # username is provided as env variable and in config file,
            # env variable should be chosen
            validate_command(
                (test,),
                None,
                "pw_env",
                "invalid password from env",
            )

        # username is provided in config file,
        # it should be read correctly
        validate_command(
            (test,),
            None,
            "pw_file",
            "invalid password from file",
        )


def test_account_name_priority(fs: FakeFilesystem):
    """username is processed correctly, in correct proirity from different sources"""
    opt = _common_options[0]  # account_name option

    # helper command, dumps all options it received
    @command()
    @opt
    def test(**kwargs):
        echo(dumps(kwargs))

    def validate_command(command: Tuple, expected_value: str, err_msg: str):
        result = runner.invoke(*command)
        assert result.exit_code == 0, "non-zero exit code for "
        config = loads(result.output)
        assert "username" in config, "missing username command option"
        assert config["username"] == expected_value, err_msg

    with create_config_file(fs, {"username": "un_file"}):
        runner = CliRunner()

        with mock.patch.dict(environ, {"FIREBOLT_USERNAME": "un_env"}):
            # username is provided as option, env variable and in config file,
            # option should be chosen
            validate_command(
                (test, ["--username", "un_option"]),
                "un_option",
                "invalid username from option",
            )

            validate_command(
                (test, ["-u", "un_option"]),
                "un_option",
                "invalid username from option",
            )

            # username is provided as env variable and in config file,
            # env variable should be chosen
            validate_command(
                (test,),
                "un_env",
                "invalid username from env",
            )

        # username is provided in config file,
        # it should be read correctly
        validate_command(
            (test,),
            "un_file",
            "invalid username from file",
        )


def test_parameters_missing(fs: FakeFilesystem):
    for opt, option_name in zip(
        _common_options, ("username", "password", "account_name")
    ):
        # helper command, dumps all options it received
        @command()
        @opt
        def test(**kwargs):
            echo(dumps(kwargs))

        result = CliRunner().invoke(test)
        assert result.exit_code == 2, f"Invalid return code for missing {option_name}"
        assert (
            f"Missing option {option_name}" in result.stdout
        ), "Invalid missing parameter message"
