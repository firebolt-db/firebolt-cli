from click import command, echo, option, group

from firebolt.common import Settings
from firebolt.service.manager import ResourceManager

from firebolt_cli.common_options import common_options


@group()
def database() -> None:
    pass


@command()
@common_options
@option("--database-name", help="New database name")
@option("--region", help="Region for the new database", default="us-east-1")
def create(**raw_config_options: dict) -> None:
    settings = Settings(
        server=raw_config_options["api_endpoint"],
        user=raw_config_options["username"],
        password=raw_config_options["password"],
        default_region=raw_config_options["region"],
    )

    try:
        rm = ResourceManager(settings=settings)

        database = rm.databases.create(
            name=raw_config_options["database_name"],
            region=raw_config_options["region"],
        )

        echo("Database {name} is successfully created".format(name=database.name))
    except RuntimeError as err:
        echo(err)


database.add_command(create)
