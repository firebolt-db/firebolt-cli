from configparser import ConfigParser
from os import path

import click
from click import UsageError, command, echo, option, prompt

from firebolt_cli.common_options import (
    config_file,
    config_section,
    option_engine_name_url,
)
from firebolt_cli.utils import read_from_file


def read_config_file() -> dict:
    """
    :return: dict with parameters from config file, or empty dict if no parameters found
    """
    config = ConfigParser(interpolation=None)
    if path.exists(config_file):
        config.read(config_file)
        if config.has_section(config_section):
            return dict((k, v) for k, v in config[config_section].items())

    return {}


def update_config_file(**kwargs: str) -> None:
    config = ConfigParser(interpolation=None)
    if path.exists(config_file):
        config.read(config_file)
        message = "Updated existing config file"
    else:
        message = "Created new config file"

    if config.has_section(config_section):
        # override engine_name with engine_url and vice versa
        if "engine_name" in kwargs and "engine_url" in config[config_section]:
            config[config_section].pop("engine_url")
        if "engine_url" in kwargs and "engine_name" in config[config_section]:
            config[config_section].pop("engine_name")

        config[config_section].update(kwargs)
    else:
        config[config_section] = kwargs

    with open(config_file, "w") as cf:
        config.write(cf)
    echo(message)


@command()
@option("-u", "--username", help="Firebolt username")
@option("--account-name", help="Name of Firebolt account")
@option("--database-name", help="Database to use for SQL queries")
@option(
    "--password-file",
    help="Path to the file, where password is stored",
    default=None,
    type=click.Path(exists=True),
)
@option("--api-endpoint", hidden=True)
@option_engine_name_url(read_from_config=False)
def configure(**raw_config_options: str) -> None:
    """
    Store firebolt configuration parameters in config file
    """
    config = {k: v for k, v in raw_config_options.items() if v}

    if "engine_name" in config and "engine_url" in config:
        raise UsageError(
            "engine-name and engine-url are mutually exclusive options. "
            "Provide only one"
        )

    if config:
        if "password_file" in config:
            password = read_from_file(config["password_file"])
            config["password"] = password if password else ""
            config.pop("password_file")
    else:
        prev_config = read_config_file()

        keys = ("username", "password", "account_name", "database_name")
        skip_message = (
            prev_config.get("username", None),
            "************" if "password" in prev_config else None,
            prev_config.get("account_name", None),
            prev_config.get("database_name", None),
        )

        for k, message in zip(keys, skip_message):
            value = prompt(
                f'{k.capitalize().replace("_", " ")} [{message}]',
                hide_input=k == "password",
                default=prev_config.get(k, ""),
                show_default=False,
            )
            config[k] = value

        # Prompt for engine name or url
        prev_engine_name_or_url = prev_config.get(
            "engine_name", None
        ) or prev_config.get("engine_url", None)
        value = prompt(
            f"Engine name or url [{prev_engine_name_or_url}]",
            hide_input=False,
            default=prev_engine_name_or_url,
            show_default=False,
        )

        # Decide whether to store the value as engine_name or engine_url
        # '.' symbol should always be in url and cannot be in engine_name
        if "." in value:
            config["engine_url"] = value
        else:
            config["engine_name"] = value

    update_config_file(**config)
