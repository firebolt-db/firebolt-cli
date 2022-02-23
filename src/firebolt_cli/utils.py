import json
import os
import sys
from configparser import ConfigParser
from functools import wraps
from typing import Callable, Dict, Optional, Sequence

import keyring
from appdirs import user_config_dir
from click import echo
from firebolt.common import Settings
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from keyring.errors import KeyringError
from tabulate import tabulate

config_file = os.path.join(user_config_dir(), "firebolt.ini")
config_section = "firebolt-cli"


def prepare_execution_result_line(
    data: Sequence, header: Sequence, use_json: bool = False
) -> str:
    """
    return the string representation of data in either json or tabular formats.
    In case of json, the result is dict
    In case of tabular, the result is table with headers in the first column
    """

    if len(data) != len(header):
        raise ValueError("data and header have different length")

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
            raise ValueError("data and header have different length")

    if use_json:
        return json.dumps([dict(zip(header, d)) for d in data], indent=4)
    else:
        return tabulate(data, headers=header, tablefmt="grid")


def construct_resource_manager(**raw_config_options: str) -> ResourceManager:
    """
    Propagate raw_config_options to the settings and construct a resource manager
    """

    settings_dict = {
        "server": raw_config_options["api_endpoint"],
        "default_region": raw_config_options.get("region", ""),
        "account_name": raw_config_options["account_name"],
    }

    rm = ResourceManager(
        Settings(
            **settings_dict,
            user=raw_config_options["username"],
            password=raw_config_options["password"],
        )
    )
    return rm


def convert_bytes(num: Optional[float]) -> str:
    """
    this function will convert bytes to KB, MB, GB, TB, PB, EB, ZB, YB
    """
    if num is None:
        return ""

    if num < 0:
        raise ValueError("Byte size cannot be negative")

    def format_output(bytes: float, dim: str) -> str:
        return "{} {}".format(f"{bytes:.2f}".rstrip("0").rstrip("."), dim)

    step_unit = 1024

    for x in ["KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]:
        num /= step_unit
        if num < step_unit:
            return format_output(num, x)

    return format_output(num, x[::-1])


def string_to_int_or_none(val: Optional[str]) -> Optional[int]:
    return int(val) if val else None


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

    # Try to update password in keyring first, and only if failed in config
    if (
        "password" in kwargs
        and kwargs["password"] is not None
        and set_keyring_param("password", kwargs["password"])
    ):
        del kwargs["password"]

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


def exit_on_firebolt_exception(func: Callable) -> Callable:
    """
    Decorator which catches FireboltError and RuntimeError and exits the programms
    """

    @wraps(func)
    def decorator(*args: str, **kwargs: str) -> None:
        try:
            func(*args, **kwargs)
        except (FireboltError, RuntimeError) as err:
            echo(err, err=True)
            sys.exit(1)

    return decorator
