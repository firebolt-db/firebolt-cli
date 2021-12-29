from configparser import ConfigParser
from os import environ, path
from typing import Callable, Optional

from appdirs import user_config_dir
from click import Context, MissingParameter, Parameter, option, prompt
from firebolt.client import DEFAULT_API_URL

config_file = path.join(user_config_dir(), "firebolt.ini")
config_section = "firebolt-cli"

_config: Optional[ConfigParser] = None


def read_config_key(key: str) -> Optional[str]:
    global _config
    # read config once
    if not _config:
        # return None if there is no config file
        if not path.exists(config_file):
            return None
        _config = ConfigParser()
        _config.read(config_file)

    return _config.get(config_section, key, fallback=None)


def default_from_config_file(default: Optional[str] = None) -> Callable:
    def inner(ctx: Context, param: Parameter, value: Optional[str]) -> str:
        # type check
        assert param.name
        value = value or read_config_key(param.name) or default
        if not value:
            raise MissingParameter(ctx=ctx, param=param, param_hint=param.name)
        return value

    return inner


def password_from_config_file(ctx: Context, param: Parameter, value: bool) -> str:
    # type check
    assert param.name
    # user asked to prompt for password
    if value:
        return prompt("Password", type=str, hide_input=True)
    pw_value = environ.get("FIREBOLT_PASSWORD") or read_config_key(param.name)
    if not pw_value:
        raise MissingParameter(ctx=ctx, param=param, param_hint=param.name)
    return pw_value


_common_options = [
    option(
        "-u",
        "--username",
        envvar="FIREBOLT_USERNAME",
        callback=default_from_config_file(),
        help="Firebolt username",
    ),
    option(
        "-p",
        "--password",
        is_flag=True,
        callback=password_from_config_file,
        help="Firebolt password",
    ),
    option(
        "--account-name",
        envvar="FIREBOLT_ACCOUNT_NAME",
        callback=default_from_config_file(),
        help="Name of Firebolt account",
    ),
    option(
        "--api-endpoint",
        envvar="FIREBOLT_API_ENDPOINT",
        callback=default_from_config_file(DEFAULT_API_URL),
        hidden=True,
    ),
]


def common_options(command: Callable) -> Callable:
    for add_option in reversed(_common_options):
        command = add_option(command)
    return command
