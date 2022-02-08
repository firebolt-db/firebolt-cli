from configparser import ConfigParser
from functools import update_wrapper
from os import environ, path
from typing import Any, Callable, List, Optional, TypeVar, cast

from appdirs import user_config_dir
from click import (
    BadOptionUsage,
    Context,
    MissingParameter,
    Parameter,
    get_current_context,
    option,
    prompt,
)
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
        _config = ConfigParser(interpolation=None)
        _config.read(config_file)

    return _config.get(config_section, key, fallback=None)


def default_from_config_file(
    default: Optional[str] = None, required: bool = True
) -> Callable:
    def inner(ctx: Context, param: Parameter, value: Optional[str]) -> Optional[str]:
        # type check
        assert param.name
        value = value or read_config_key(param.name) or default
        if required and not value:
            raise MissingParameter(ctx=ctx, param=param, param_hint=f"--{param.name}")
        return value

    return inner


def password_from_config_file(
    ctx: Context, param: Parameter, value: bool
) -> Optional[str]:
    # type check
    assert param.name
    # user asked to prompt for password
    if value:
        return prompt("Password", type=str, hide_input=True)
    pw_value = environ.get("FIREBOLT_PASSWORD") or read_config_key(param.name)
    if not pw_value:
        raise MissingParameter(ctx=ctx, param=param, param_hint=f"--{param.name}")
    return pw_value


F = TypeVar("F", bound=Callable[..., Any])


def validate_json_option(f: F) -> F:
    def inner(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("json", False) and not kwargs.get("yes", False):
            raise BadOptionUsage(
                "json", "--json should be used with -y", get_current_context()
            )
        return f(*args, **kwargs)

    return update_wrapper(cast(F, inner), f)


_common_options: List[Callable] = [
    option(
        "-u",
        "--username",
        envvar="FIREBOLT_USERNAME",
        callback=default_from_config_file(required=True),
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
        callback=default_from_config_file(required=True),
        help="Name of Firebolt account",
    ),
    option(
        "--api-endpoint",
        envvar="FIREBOLT_API_ENDPOINT",
        callback=default_from_config_file(DEFAULT_API_URL, required=False),
        hidden=True,
    ),
]


def common_options(command: Callable) -> Callable:
    for add_option in reversed(_common_options):
        command = add_option(command)
    return command


def json_option(command: Callable) -> Callable:
    return option(
        "--json",
        help="Provide output in json format",
        default=False,
        is_flag=True,
        multiple=False,
    )(command)


def option_engine_name_url(read_from_config: bool = False) -> Callable:
    def option_engine_name_url_inner(command: Callable) -> Callable:
        command = option(
            "--engine-name",
            help="Name of engine to use for SQL queries. "
            "Incompatible with --engine-url",
            required=False,
            default=None,
            callback=default_from_config_file(required=False)
            if read_from_config
            else None,
        )(command)

        command = option(
            "--engine-url",
            help="Url of engine to use for SQL queries. "
            "Incompatible with --engine-name",
            required=False,
            default=None,
            callback=default_from_config_file(required=False)
            if read_from_config
            else None,
        )(command)

        return command

    return option_engine_name_url_inner
