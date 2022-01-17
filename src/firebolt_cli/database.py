import os
import sys

from click import command, echo, group, option
from firebolt.common import Settings
from firebolt.service.manager import ResourceManager
from pydantic import ValidationError

from firebolt_cli.common_options import common_options
from firebolt_cli.utils import prepare_execution_result


@group()
def database() -> None:
    """
    Manage the databases using the python-cli
    """


@command()
@common_options
@option("--name", help="New database name", type=str)
@option(
    "--description",
    help="Database textual description up to 64 characters",
    type=str,
    default="",
)
@option("--region", help="Region for the new database", default="us-east-1", type=str)
def create(**raw_config_options: str) -> None:
    """
    Create a new database
    """
    settings = Settings(
        server=raw_config_options["api_endpoint"],
        user=raw_config_options["username"],
        password=raw_config_options["password"],
        default_region=raw_config_options["region"],
    )

    try:
        rm = ResourceManager(settings=settings)

        database = rm.databases.create(
            name=raw_config_options["name"],
            description=raw_config_options["description"],
            region=raw_config_options["region"],
        )

        echo(
            "Database {name} is successfully created".format(name=database.name),
            err=True,
        )
        echo(
            prepare_execution_result(
                dict(
                    {
                        "name": database.name,
                        "description": database.description,
                        "create_time": str(database.create_time),
                    }
                ),
                use_json=bool(raw_config_options["json"]),
            )
        )

    except RuntimeError as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)
    except ValidationError as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


database.add_command(create)
