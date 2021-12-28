from configparser import ConfigParser
from os import path

from click import command, echo, prompt

from firebolt_cli.common_options import config_file, config_section


def update_config_file(**kwargs):
    config = ConfigParser()
    if path.exists(config_file):
        config.read(config_file)
        message = "Updated existing config file"
    else:
        message = "Created new config file"

    if config.has_section(config_section):
        # override engine_name with engine_url and vice versa
        if "engine_name" in kwargs:
            config[config_section].pop("engine_url")
        if "engine_url" in kwargs:
            config[config_section].pop("engine_name")

        config[config_section].update(kwargs)
    else:
        config[config_section] = kwargs

    with open(config_file, "w") as cf:
        config.write(cf)
    echo(message)


@command()
def configure(**kwargs):
    echo("Config file: " + config_file)

    keys = ("username", "password", "account_name", "database_name")
    config = {}
    skip_message = " (press Enter to skip)"

    for k in keys:
        value = prompt(
            k.capitalize().replace("_", " ") + skip_message,
            hide_input=k == "password",
            default="",
            show_default=False,
        )
        if value:
            config[k] = value

    engine_name = prompt(
        "Engine name(press Enter if you want to enter engine url instead)",
        default="",
        show_default=False,
    )
    if engine_name:
        config["engine_name"] = engine_name
    else:
        engine_url = prompt("Engine URL" + skip_message, default="", show_default=False)
        if engine_url:
            config["engine_url"] = engine_url

    update_config_file(**config)
