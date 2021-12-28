from configparser import ConfigParser
from os import path
from typing import Callable, Optional

from appdirs import user_config_dir
from click import Context, MissingParameter, Parameter, option, prompt

config_file = path.join(user_config_dir(), "firebolt.config")
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

    return _config.get(config_section, key)


def default_from_config_file(ctx: Context, param: Parameter, value: str):
    value = value or read_config_key(param.name)
    if not value:
        raise MissingParameter(ctx=ctx, param=param)
    return value


def password_from_config_file(ctx: Context, param: Parameter, value: bool):
    # user asked to prompt for password
    if value:
        return prompt("Password", type=str, hide_input=True)
    value = read_config_key(param.name)
    if not value:
        raise MissingParameter(ctx=ctx, param=param)
    return value


_common_options = [
    option(
        "-u",
        "--username",
        envvar="FIREBOLT_USERNAME",
        callback=default_from_config_file,
    ),
    option(
        "-p",
        "--password",
        envvar="FIREBOLT_PASSWORD",
        is_flag=True,
        callback=password_from_config_file,
    ),
    option(
        "--account_name",
        envvar="FIREBOLT_ACCOUNT_NAME",
        callback=default_from_config_file,
    ),
]


def common_options(command: Callable):
    for add_option in reversed(_common_options):
        command = add_option(command)
    return command
