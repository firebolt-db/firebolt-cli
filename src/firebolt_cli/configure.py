from click import Context, echo, group, option, pass_context, prompt

from firebolt_cli.utils import (
    exit_on_firebolt_exception,
    read_config,
    update_config,
)


@group(
    name="configure",
    short_help="Store firebolt configuration (alias: config)",
    invoke_without_command=True,
)
@option("-u", "--username", help="The email address used for connecting to Firebolt.")
@option(
    "-p",
    "--password",
    is_flag=True,
    default=False,
    help="Opens a protected prompt to enter the Firebolt password.",
)
@option("--account-name", help="The name of the Firebolt account.")
@option(
    "--database-name", help="The name of the database you would like to connect to."
)
@option("--api-endpoint", hidden=True)
@option("--engine-name", help="The name or URL of the engine to use.")
@exit_on_firebolt_exception
@pass_context
def configure(ctx: Context, **raw_config_options: str) -> None:
    """
    Store firebolt configuration parameters in config file.
    """
    if ctx.invoked_subcommand is None:
        config = {k: v for k, v in raw_config_options.items() if v is not None}

        if config["password"]:
            config["password"] = prompt("Password", type=str, hide_input=True)
        else:
            del config["password"]

        if len(config) == 0:
            prev_config = read_config()

            keys = (
                "username",
                "password",
                "account_name",
                "database_name",
                "engine_name",
            )
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

        if config.get("account_name", None) is not None:
            config["account_name"] = config["account_name"].lower()

        update_config(**config)
        echo("Successfully updated firebolt-cli configuration")


@configure.command()
@exit_on_firebolt_exception
def reset() -> None:
    """
    Reset all previously set configurations.
    """
    update_config(
        username="", password="", account_name="", database_name="", engine_name=""
    )
    echo("Successfully reset firebolt-cli configuration")
