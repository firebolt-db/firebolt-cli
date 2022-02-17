from configparser import ConfigParser
from contextlib import contextmanager
from io import StringIO
from json import dumps, loads
from os import environ
from typing import Callable, List, Optional, Tuple
from unittest import mock

from click import MissingParameter, command, echo
from click.testing import CliRunner
from firebolt.client import DEFAULT_API_URL
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt_cli.common_options import _common_options
from firebolt_cli.utils import config_file, config_section, update_config


@contextmanager
def create_config_file(fs: FakeFilesystem, config: dict) -> None:
    # make sure config will be flushed not to be reused in other tests
    cp = ConfigParser(interpolation=None)
    cp[config_section] = config
    content = StringIO()
    cp.write(content)

    fs.create_file(config_file, contents=content.getvalue())

    yield

    fs.remove(config_file)


def generic_test_parameter_priority(
    fs: FakeFilesystem, name: str, cli_names: List[str], opt: Callable
):
    # helper command, dumps all options it received
    @command()
    @opt
    def test(**kwargs):
        echo(dumps(kwargs))

    def validate_command(command: Tuple, expected_value: str, err_msg: str):
        result = CliRunner().invoke(*command)
        assert result.exit_code == 0, "non-zero exit code"
        config = loads(result.output)
        assert name in config, f"missing {name} command option"
        assert config[name] == expected_value, err_msg

    with create_config_file(fs, {name: "file"}):

        with mock.patch.dict(environ, {f"FIREBOLT_{name.upper()}": "env"}):
            for cli_name in cli_names:
                # parameter is provided as option, env variable and in config file,
                # option should be chosen
                validate_command(
                    (test, [f"{cli_name}", "option"]),
                    "option",
                    f"invalid {name} from option",
                )

            # parameter is provided as env variable and in config file,
            # env variable should be chosen
            validate_command(
                (test,),
                "env",
                f"invalid {name} from env",
            )

        # parameter is provided in config file,
        # it should be read correctly
        validate_command(
            (test,),
            "file",
            f"invalid {name} from file",
        )


def test_username_priority(fs: FakeFilesystem):
    """username is processed correctly, in correct proirity from different sources"""
    generic_test_parameter_priority(
        fs, "username", ["-u", "--username"], _common_options[0]
    )


def test_account_name_priority(fs: FakeFilesystem):
    """
    account_name is processed correctly, in correct proirity from different sources
    """
    generic_test_parameter_priority(
        fs, "account_name", ["--account-name"], _common_options[2]
    )


def test_api_endpoint_priority(fs: FakeFilesystem):
    """
    account_name is processed correctly, in correct proirity from different sources
    """
    generic_test_parameter_priority(
        fs, "api_endpoint", ["--api-endpoint"], _common_options[3]
    )


def test_password_priority(fs: FakeFilesystem):
    """username is processed correctly, in correct proirity from different sources"""
    opt = _common_options[1]  # password option

    SPECIAL_CHARACTERS = " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
    # helper command, dumps all options it received
    @command()
    @opt
    def test(**kwargs):
        echo(dumps(kwargs))

    def validate_command(
        command: Tuple, input: Optional[str], expected_value: str, err_msg: str
    ):
        result = CliRunner().invoke(*command, input=input)
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

    with create_config_file(fs, {}):
        update_config(password="pw_config" + SPECIAL_CHARACTERS)
        # password is provided as option and in config file,
        # option should be chosen
        validate_command(
            (test, ["--password"]),
            "pw_option" + SPECIAL_CHARACTERS,
            "pw_option" + SPECIAL_CHARACTERS,
            "invalid password from option",
        )

        validate_command(
            (test, ["-p"]),
            "pw_option" + SPECIAL_CHARACTERS,
            "pw_option" + SPECIAL_CHARACTERS,
            "invalid password from option",
        )

        # password is provided in config file,
        # config file should be chosen
        validate_command(
            (test,),
            None,
            "pw_config" + SPECIAL_CHARACTERS,
            "invalid password from config file",
        )


def test_parameters_missing(fs: FakeFilesystem):
    def check_empty_option(opt: Callable, option_name: str):
        # helper command, dumps all options it received
        @command()
        @opt
        def test(**kwargs):
            echo(dumps(kwargs))

        result = CliRunner().invoke(test)
        assert (
            result.exit_code == MissingParameter.exit_code
        ), f"invalid return code for missing {option_name}"
        assert (
            f"Missing option --{option_name}" in result.stdout
        ), "invalid missing parameter message"

    for opt, option_name in zip(
        _common_options[:3], ("username", "password", "account_name")
    ):
        check_empty_option(opt, option_name)

        # create config file without current option
        with create_config_file(fs, {}):
            check_empty_option(opt, option_name)


def test_api_endpoint_missing(fs: FakeFilesystem):
    def check_empty_option():
        opt = _common_options[3]  # api-endpoint

        @command()
        @opt
        def test(**kwargs):
            echo(dumps(kwargs))

        result = CliRunner().invoke(test)
        assert result.exit_code == 0, "non-zero exit code for missing api-endpoint"
        config = loads(result.stdout)
        assert "api_endpoint" in config, "missing api-endpoint parameter"
        assert (
            config["api_endpoint"] == DEFAULT_API_URL
        ), "invalid api-endpoint default value"

    check_empty_option()

    # create config file without current option
    with create_config_file(fs, {}):
        check_empty_option()
