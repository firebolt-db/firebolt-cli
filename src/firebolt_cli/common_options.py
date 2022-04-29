from os import environ
from typing import Callable, List, Optional

from click import Context, MissingParameter, Parameter, option, prompt
from firebolt.client import DEFAULT_API_URL

from firebolt_cli.utils import read_config


def default_from_config_file(
    default: Optional[str] = None, required: bool = True
) -> Callable:
    def inner(ctx: Context, param: Parameter, value: Optional[str]) -> Optional[str]:
        # type check
        assert param.name

        value = value or read_config().get(param.name, None) or default
        if required and not value:
            raise MissingParameter(
                ctx=ctx,
                param=param,
                param_hint="--{}".format(param.name.replace("_", "-")),
            )
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

    pw_value = environ.get("FIREBOLT_PASSWORD") or read_config().get("password", None)
    if not pw_value:
        raise MissingParameter(
            ctx=ctx, param=param, param_hint="--{}".format(param.name.replace("_", "-"))
        )

    return pw_value


_common_options: List[Callable] = [
    option(
        "-u",
        "--username",
        envvar="FIREBOLT_USERNAME",
        callback=default_from_config_file(required=True),
        help="The username used for connecting to Firebolt.",
    ),
    option(
        "-p",
        "--password",
        is_flag=True,
        callback=password_from_config_file,
        help=" The password used for connecting to Firebolt.",
    ),
    option(
        "--account-name",
        envvar="FIREBOLT_ACCOUNT_NAME",
        callback=default_from_config_file(required=False),
        help="The name of the Firebolt account.",
    ),
    option(
        "--api-endpoint",
        envvar="FIREBOLT_API_ENDPOINT",
        callback=default_from_config_file(DEFAULT_API_URL, required=False),
        hidden=True,
    ),
    option(
        "--access-token",
        envvar="FIREBOLT_ACCESS_TOKEN",
        help="Firebolt token for authentication. "
        "If the access-token fails, the username/password will be used instead.",
        required=False,
    ),
]


def common_options(command: Callable) -> Callable:
    for add_option in reversed(_common_options):
        command = add_option(command)
    return command


def json_option(command: Callable) -> Callable:
    return option(
        "--json",
        help="Provide output in JSON format.",
        default=False,
        is_flag=True,
        multiple=False,
    )(command)
