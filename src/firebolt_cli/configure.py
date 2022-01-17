from configparser import ConfigParser
from os import path

from click import UsageError, command, echo, option, prompt

from firebolt_cli.common_options import config_file, config_section


def update_config_file(**kwargs: str) -> None:
    config = ConfigParser()
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
@option("--api-endpoint", hidden=True)
@option(
    "--engine-name",
    help="Name of engine to use for SQL queries. Incompatible with --engine-url",
)
@option(
    "--engine-url",
    help="Url of engine to use for SQL queries. Incompatible with --engine-name",
)
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

    keys = ("username", "password", "account_name", "database_name")
    skip_message = " (press Enter to skip)"

    for k in keys:
        if k not in config:
            value = prompt(
                k.capitalize().replace("_", " ") + skip_message,
                hide_input=k == "password",
                default="",
                show_default=False,
            )
            if value:
                config[k] = value

    if "engine_name" not in config and "engine_url" not in config:
        engine_name = prompt(
            "Engine name (press Enter if you want to enter engine url instead)",
            default="",
            show_default=False,
        )
        if engine_name:
            config["engine_name"] = engine_name
        else:
            engine_url = prompt(
                "Engine URL" + skip_message, default="", show_default=False
            )
            if engine_url:
                config["engine_url"] = engine_url

    update_config_file(**config)
