import json
import os
import sys
from configparser import ConfigParser
from typing import Optional, Sequence

import keyring
from appdirs import user_config_dir
from firebolt.common import Settings
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

    settings = Settings(
        server=raw_config_options["api_endpoint"],
        user=raw_config_options["username"],
        password=raw_config_options["password"],
        default_region=raw_config_options.get("region", ""),
    )

    return ResourceManager(settings)


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


def set_password(password: str) -> None:
    """
    Set the password using keyring,
    if unavailable uses the config file as a fallback option
    """
    try:
        keyring.set_password("firebolt-cli", "firebolt-cli", password)
    except KeyringError:
        update_config_file(password=password)


def get_password() -> Optional[str]:
    """
    Get the password from keyring,
    if unavailable tries to get it from the config file
    """
    try:
        return keyring.get_password("firebolt-cli", "firebolt-cli")
    except KeyringError:
        password = read_config_file().get("password", None)
        return password if password else None


def delete_password() -> None:
    """
    Delete the password from keyring and the config file
    """
    try:
        keyring.delete_password("firebolt-cli", "firebolt-cli")
    except KeyringError:
        pass

    update_config_file(password="")


def read_config_file() -> dict:
    """
    :return: dict with parameters from config file, or empty dict if no parameters found
    """
    config = ConfigParser(interpolation=None)
    if os.path.exists(config_file):
        config.read(config_file)
        if config.has_section(config_section):
            return dict((k, v) for k, v in config[config_section].items())

    return {}


def update_config_file(**kwargs: str) -> str:
    """

    :param kwargs:
    :return:
    """
    config = ConfigParser(interpolation=None)
    if os.path.exists(config_file):
        config.read(config_file)
        message = "Updated existing config file"
    else:
        message = "Created new config file"

    if config.has_section(config_section):
        config[config_section].update(kwargs)
    else:
        config[config_section] = kwargs

    with open(config_file, "w") as cf:
        config.write(cf)

    return message
