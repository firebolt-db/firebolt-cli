import json
import os
import sys
from configparser import ConfigParser
from typing import Optional, Sequence

import keyring
from appdirs import user_config_dir
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

    If access_token could be extracted from the config, try to access using it,
    use username, password as fallback option
    """

    settings_dict = {
        "server": raw_config_options["api_endpoint"],
        "default_region": raw_config_options.get("region", ""),
    }

    token = read_config().get("token", None)
    if token is not None:
        settings_dict["access_token"] = token
        try:
            return ResourceManager(Settings(**settings_dict))
        except (FireboltError, RuntimeError):
            del settings_dict["access_token"]

    settings_dict["user"] = raw_config_options["username"]
    settings_dict["password"] = raw_config_options["password"]

    rm = ResourceManager(Settings(**settings_dict))
    update_config(token=rm.client.auth.token)
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


def read_config() -> dict:
    """
    :return: dict with parameters from config file, or empty dict if no parameters found
    """
    config_dict = {}

    config = ConfigParser(interpolation=None)
    if os.path.exists(config_file):
        config.read(config_file)
        if config.has_section(config_section):
            config_dict = dict((k, v) for k, v in config[config_section].items())

    for param in ["token", "password"]:
        try:
            value = keyring.get_password("firebolt-cli", param)
            if value and len(value) != 0:
                config_dict[param] = value
        except KeyringError:
            continue

    return dict({(k, v) for k, v in config_dict.items() if v and len(v) != 0})


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
    Update the config file (or use the keyring for updating token and password)
    if a parameter set to None, the parameter will not be updates
    To delete the parameter, it should be set to empty string

    Note: token cannot be updated if other parameters are not None

    :param kwargs:
    :return:
    """

    # Invalidate the current token if one of the parameters is set
    if any(
        [i in kwargs for i in ["password", "username", "account_name", "api_endpoint"]]
    ):
        set_keyring_param("token", "")
        kwargs["token"] = ""

    # Try to update token and password in keyring first, and only if failed in config
    for param in ["token", "password"]:
        if (
            param in kwargs
            and kwargs[param] is not None
            and set_keyring_param(param, kwargs[param])
        ):
            del kwargs[param]

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
