import json
import os
import re
import sys
from configparser import ConfigParser
from functools import lru_cache, wraps
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Type

import keyring
import sqlparse  # type: ignore
from appdirs import user_config_dir
from click import Command, Context, Group, echo
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.common import Settings
from firebolt.common.exception import FireboltError
from firebolt.db.connection import Connection, connect
from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt_ingest.aws_settings import (
    AWSCredentials,
    AWSCredentialsKeySecret,
    AWSCredentialsRole,
)
from httpx import HTTPStatusError
from keyring.errors import KeyringError
from tabulate import tabulate

config_file = os.path.join(user_config_dir(), "firebolt.ini")
config_section = "firebolt-cli"


def construct_shortcuts(shortages: dict) -> Type[Group]:
    class AliasedGroup(Group):
        def get_command(self, ctx: Context, cmd_name: str) -> Optional[Command]:
            rv = Group.get_command(self, ctx, cmd_name)
            if rv is not None:
                return rv

            matches = [
                x for x in self.list_commands(ctx) if x in shortages.get(cmd_name, [])
            ]

            if not matches:
                return None

            assert len(matches) == 1
            return Group.get_command(self, ctx, matches[0])

    return AliasedGroup


def format_short_statement(statement: str, truncate_long_string: int = 80) -> str:
    """
    Format a complex query into a single line with
    stripped comments and excessive whitespaces

    Args:
        statement: a valid sql statement
        truncate_long_string: if set to positive integer strings longer, that
        the specified value will be truncated and "..." will be added

    Returns: a formatted string

    """
    statement = sqlparse.format(
        str(statement), strip_comments=True, use_space_around_operators=True
    )

    statement = statement.replace("\n", " ").replace("\t", " ").strip()

    # strip consecutive whitespaces
    statement = re.sub(" +", " ", statement)

    if 0 < truncate_long_string < len(statement):
        return statement[:truncate_long_string] + " ..."

    return statement


def prepare_execution_result_line(
    data: Sequence, header: Sequence, use_json: bool = False
) -> str:
    """
    return the string representation of data in either json or tabular formats.
    In case of json, the result is dict
    In case of tabular, the result is table with headers in the first column
    """

    if len(data) != len(header):
        raise ValueError("Data and header have different length.")

    if use_json:
        return json.dumps(dict(zip(header, data)), indent=4)
    else:
        return tabulate(list(zip(header, data)), tablefmt="grid")


def prepare_execution_result_table(
    data: Sequence[Sequence], header: Sequence, use_json: bool = False
) -> str:
    """
    return the string representation of data in either json or tabular formats
    In case of json, the result is list of dicts
    In case of tabular, the result is table with headers in the first row
    """
    for d in data:
        if len(d) != len(header):
            raise ValueError("Data and header have different length.")

    if use_json:
        return json.dumps([dict(zip(header, d)) for d in data], indent=4)
    else:
        return tabulate(data, headers=header, tablefmt="grid")


def construct_resource_manager(**raw_config_options: str) -> ResourceManager:
    """
    Propagate raw_config_options to the settings and construct a resource manager
    :rtype: object
    """
    return ResourceManager(
        auth=ClientCredentials(raw_config_options["client_id"], raw_config_options["client_secret"]),
        account_name=raw_config_options["account_name"].lower(),
        api_endpoint=raw_config_options["api_endpoint"],
    )


def to_human_readable(num: Optional[float], step_unit: int, labels: List[str]) -> str:
    """
    converts a long number to a short human-readable string representation
    e.g. 1233212 -> 1.2 M

    Args:
        num: number to be converted
        step_unit: threshold for moving to the next label
        labels: labels, that should be used for each next step

    """
    if num is None:
        return ""

    if num < 0:
        raise ValueError("Value cannot be negative")

    def format_output(bytes: float, dim: str) -> str:
        return "{} {}".format(f"{bytes:.2f}".rstrip("0").rstrip("."), dim)

    for x in labels:
        num /= step_unit
        if num < step_unit:
            return format_output(num, x)

    return format_output(num, x)


def convert_bytes(num: Optional[float]) -> str:
    """
    this function will convert bytes to KB, MB, GB, TB, PB, EB, ZB, YB
    """
    return to_human_readable(
        num, step_unit=1024, labels=["KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    )

def convert_price_per_hour(cents: Optional[float]) -> str:
    return f"{cents:.2f}$/hour" if cents else "-"


def convert_num_human_readable(num: Optional[float]) -> str:
    """
    this function will convert number to human-readable form
    """
    return to_human_readable(num, step_unit=1000, labels=["K", "M", "G", "T"])


def read_from_file(fpath: Optional[str]) -> Optional[str]:
    """
    read from file, if fpath is not None, otherwise return empty string
    """
    if fpath is None:
        return None

    with open(fpath, "r") as f:
        return f.read() or None


def read_from_stdin_buffer() -> Optional[str]:
    """
    read from buffer if stdin file descriptor is open, otherwise return empty string
    """
    if sys.stdin.isatty():
        return None

    return sys.stdin.buffer.read().decode("utf-8") or None


@lru_cache()
def read_config() -> Dict[str, str]:
    """
    :return: dict with parameters from config file, or empty dict if no parameters found
    """
    config_dict: Dict[str, Optional[str]] = {}

    config = ConfigParser(interpolation=None)
    if os.path.exists(config_file):
        config.read(config_file)
        if config.has_section(config_section):
            config_dict = dict((k, v) for k, v in config[config_section].items())

    try:
        value = keyring.get_password("firebolt-cli", "password")
        if value and len(value) != 0:
            config_dict["password"] = value
    except KeyringError:
        pass

    return dict({(k, v) for k, v in config_dict.items() if v and len(v)})


def set_keyring_param(param: str, value: str) -> bool:
    """
    Set keyring param to value, if value is an empty string, delete the param

    :return: True if operation was successful
    """

    try:
        if value == "":
            keyring.delete_password("firebolt-cli", param)
        else:
            keyring.set_password("firebolt-cli", param, value)
    except KeyringError:
        return False

    return True


def update_config(**kwargs: str) -> None:
    """
    Update the config file (or use the keyring for updating password)
    if a parameter set to None, the parameter will not be updates
    To delete the parameter, it should be set to empty string

    :param kwargs:
    :return:
    """

    if len(kwargs):
        config = ConfigParser(interpolation=None)
        if os.path.exists(config_file):
            config.read(config_file)

        if config.has_section(config_section):
            config[config_section].update(**kwargs)
        else:
            config[config_section] = kwargs

        with open(config_file, "w") as cf:
            config.write(cf)

        read_config.cache_clear()


def exit_on_firebolt_exception(func: Callable) -> Callable:
    """
    Decorator which catches all Exceptions and exits the programms
    """

    @wraps(func)
    def decorator(*args: str, **kwargs: str) -> None:
        try:
            func(*args, **kwargs)
        except Exception as err:
            echo(err, err=True)
            sys.exit(1)

    return decorator


def create_connection(
    engine_name: Optional[str],
    database_name: str,
    client_id: str,
    client_secret: str,
    api_endpoint: str,
    account_name: Optional[str],
    **kwargs: str,
) -> Connection:
    """
    Create connection based on client id and secret provided
    """

    account_name = account_name.lower() if account_name is not None else None
    return connect(
        auth=ClientCredentials(client_id, client_secret),
        database=database_name,
        account_name=account_name,
        engine_name=engine_name,
        api_endpoint=api_endpoint
    )


def create_aws_key_secret_creds_from_environ() -> Optional[AWSCredentialsKeySecret]:
    """
    if FIREBOLT_AWS_KEY_ID/FIREBOLT_AWS_SECRET_KEY are set,
    construct AWSCredentialsKeySecret based on these variable.
    if both parameter are not set, returns None
    If only one parameter is set, raises an exception
    """
    aws_key_id = os.environ.get("FIREBOLT_AWS_KEY_ID")
    aws_secret_key = os.environ.get("FIREBOLT_AWS_SECRET_KEY")

    if aws_key_id and aws_secret_key:
        return AWSCredentialsKeySecret(
            aws_key_id=aws_key_id, aws_secret_key=aws_secret_key
        )
    elif aws_key_id or aws_secret_key:
        raise FireboltError(
            "Aws key/secret are both mandatory for a valid pair."
            "Provided only one parameter."
        )
    else:
        return None


def create_aws_role_creds_from_environ() -> Optional[AWSCredentialsRole]:
    """
    if FIREBOLT_AWS_ROLE_ARN is set returns AWSCredentialsRole from it and
    optionally from FIREBOLT_AWS_ROLE_EXTERNAL_ID
    if only FIREBOLT_AWS_ROLE_EXTERNAL_ID is set, raises an error
    """
    role_arn = os.environ.get("FIREBOLT_AWS_ROLE_ARN")
    external_id = os.environ.get("FIREBOLT_AWS_ROLE_EXTERNAL_ID")

    if role_arn:
        return AWSCredentialsRole(role_arn=role_arn, external_id=external_id)
    elif external_id:
        raise FireboltError("Aws external id is provided, but not role_arn")
    else:
        return None


def create_aws_creds_from_environ() -> Optional[AWSCredentials]:
    """
    Returns: AWSCredentials constructed from the provided environment variables;
        either from FIREBOLT_AWS_KEY_ID/FIREBOLT_AWS_SECRET_KEY pair
        or from FIREBOLT_AWS_ROLE_ARN/FIREBOLT_AWS_ROLE_EXTERNAL_ID
        in case of inconsistency raises FireboltError
    """

    key_secret_creds = create_aws_key_secret_creds_from_environ()
    role_creds = create_aws_role_creds_from_environ()

    if key_secret_creds is None and role_creds is None:
        return None

    if key_secret_creds and role_creds:
        raise FireboltError(
            "Either aws key/secret or role_arn/external_id pair "
            "should be specified. Found both."
        )

    return AWSCredentials(key_secret_creds=key_secret_creds, role_creds=role_creds)
