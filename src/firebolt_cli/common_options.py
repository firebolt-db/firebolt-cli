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


def client_secret_from_config_file(
    ctx: Context, param: Parameter, value: bool
) -> Optional[str]:
    # type check
    assert param.name

    # user asked to prompt for client secret
    if value:
        return prompt("Client Secret", type=str, hide_input=True)

    cs_value = environ.get("FIREBOLT_CLIENT_SECRET") or read_config().get(
        "client_secret", None
    )
    if not cs_value:
        raise MissingParameter(
            ctx=ctx, param=param, param_hint="--{}".format(param.name.replace("_", "-"))
        )

    return cs_value


_common_options: List[Callable] = [
    option(
        "-c",
        "--client-id",
        envvar="FIREBOLT_CLIENT_ID",
        callback=default_from_config_file(required=True),
        help="The client id used for connecting to Firebolt.",
    ),
    option(
        "-s",
        "--client-secret",
        is_flag=True,
        callback=client_secret_from_config_file,
        help="The client secret used for connecting to Firebolt.",
    ),
    option(
        "--account-name",
        envvar="FIREBOLT_ACCOUNT_NAME",
        callback=default_from_config_file(required=True),
        help="The name of the Firebolt account.",
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
        help="Provide output in JSON format.",
        default=False,
        is_flag=True,
        multiple=False,
    )(command)
