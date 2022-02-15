import click
from click import command, echo, option, prompt

from firebolt_cli.utils import (
    get_password,
    read_config_file,
    read_from_file,
    set_password,
    update_config_file,
)


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

    password = None
    if config:
        if "password_file" in config:
            password = read_from_file(config["password_file"])
            config.pop("password_file")
    else:
        prev_config = read_config_file()
        prev_password = get_password()

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
            "************" if prev_password else None,
            prev_config.get("account_name", None),
            prev_config.get("database_name", None),
            prev_config.get("engine_name", None),
        )

        for key, key_readable, message in zip(keys, keys_readable, skip_message):
            if key == "password":
                password = prompt(
                    f"{key_readable} [{message if message else None}]",
                    hide_input=True,
                    default=prev_password if prev_password else "",
                    show_default=False,
                )
            else:
                value = prompt(
                    f"{key_readable} [{message if message else None}]",
                    hide_input=key == "password",
                    default=prev_config.get(key, ""),
                    show_default=False,
                )
                config[key] = value

    echo(update_config_file(**config))

    if password:
        set_password(password=password)
