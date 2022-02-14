from configparser import ConfigParser
from os import path

import click
from click import command, echo, option, prompt

from firebolt_cli.common_options import config_file, config_section
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
    """

    :param kwargs:
    :return:
    """
    config = ConfigParser(interpolation=None)
    if path.exists(config_file):
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
@option("--engine-name", help="Name or url of the engine to use for SQL queries")
def configure(**raw_config_options: str) -> None:
    """
    Store firebolt configuration parameters in config file
    """
    config = {k: v for k, v in raw_config_options.items() if v is not None}

    if config:
        if "password_file" in config:
            password = read_from_file(config["password_file"])
            config["password"] = password if password else ""
            config.pop("password_file")
    else:
        prev_config = read_config_file()

        keys = ("username", "password", "account_name", "database_name", "engine_name")
        keys_readable = (
            "Username",
            "Password",
            "Account name",
            "Database name",
            "Engine name or URL",
        )
        skip_message = (
            prev_config.get("username", None),
            "************" if "password" in prev_config else None,
            prev_config.get("account_name", None),
            prev_config.get("database_name", None),
            prev_config.get("engine_name", None),
        )

        for key, key_readable, message in zip(keys, keys_readable, skip_message):
            value = prompt(
                f"{key_readable} [{message if message else None}]",
                hide_input=key == "password",
                default=prev_config.get(key, ""),
                show_default=False,
            )
            config[key] = value

    update_config_file(**config)
