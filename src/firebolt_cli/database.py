import os
import sys

from click import command, confirm, echo, group, option
from firebolt.common.exception import FireboltError
from firebolt.model.database import Database
from firebolt.service.manager import ResourceManager
from pydantic import ValidationError

from firebolt_cli.common_options import common_options, json_option
from firebolt_cli.utils import (
    construct_resource_manager,
    convert_bytes,
    prepare_execution_result_line,
    prepare_execution_result_table,
)


def print_db_full_information(
    rm: ResourceManager, database: Database, use_json: bool
) -> None:
    attached_engines = rm.bindings.get_engines_bound_to_database(database)
    attached_engine_names = [str(engine.name) for engine in attached_engines]

    data_size = (
        convert_bytes(database.data_size_full) if database.data_size_full else ""
    )

    echo(
        prepare_execution_result_line(
            data=[
                database.name,
                database.description,
                str(rm.regions.get_by_key(database.compute_region_key).name),
                data_size,
                str(database.create_time),
                attached_engine_names,
            ],
            header=[
                "name",
                "description",
                "region",
                "data_size",
                "create_time",
                "attached_engine_names",
            ],
            use_json=use_json,
        )
    )


@group()
def database() -> None:
    """
    Manage the databases
    """


@command()
@common_options
@option("--name", help="New database name", type=str, required=True)
@option(
    "--description",
    help="Database textual description up to 64 characters",
    type=str,
    default="",
)
@json_option
@option("--region", help="Region for the new database", default="us-east-1", type=str)
def create(**raw_config_options: str) -> None:
    """
    Create a new database
    """

    try:
        rm = construct_resource_manager(**raw_config_options)

        database = rm.databases.create(
            name=raw_config_options["name"],
            description=raw_config_options["description"],
            region=raw_config_options["region"],
        )

        if not raw_config_options["json"]:
            echo(f"Database {database.name} is successfully created")

        print_db_full_information(rm, database, bool(raw_config_options["json"]))

    except (RuntimeError, ValidationError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


@command()
@common_options
@option(
    "--name-contains",
    help="Output databases will be filtered by name_contains",
    default=None,
    type=str,
)
@json_option
def list(**raw_config_options: str) -> None:
    """
    List existing databases
    """
    try:
        rm = construct_resource_manager(**raw_config_options)

        databases = rm.databases.get_many(
            name_contains=raw_config_options["name_contains"]
        )

        if not raw_config_options["json"]:
            echo(f"Found {len(databases)} databases")

        if raw_config_options["json"] or databases:
            echo(
                prepare_execution_result_table(
                    data=[[db.name, db.description] for db in databases],
                    header=["name", "description"],
                    use_json=bool(raw_config_options["json"]),
                )
            )

    except (RuntimeError, FireboltError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


@command()
@common_options
@option("--name", help="Database name, that should be dropped", type=str, required=True)
@option(
    "--yes",
    help="Automatic yes on confirmation prompt",
    is_flag=True,
)
def drop(**raw_config_options: str) -> None:
    """
    Drop specified database
    """
    try:
        rm = construct_resource_manager(**raw_config_options)
        database = rm.databases.get_by_name(name=raw_config_options["name"])

        if raw_config_options["yes"] or confirm(
            "Do you really want to drop the database {name}?".format(
                name=raw_config_options["name"]
            )
        ):
            database.delete()
            echo(f"Drop request for database {database.name} is successfully sent")
        else:
            echo("Drop request is aborted")

    except (RuntimeError, FireboltError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


@command()
@common_options
@option(
    "--name",
    help="Database name, that should be described",
    required=True,
    type=str,
)
@json_option
def describe(**raw_config_options: str) -> None:
    """
    Describe specified database
    """
    try:
        rm = construct_resource_manager(**raw_config_options)
        database = rm.databases.get_by_name(name=raw_config_options["name"])
        print_db_full_information(rm, database, bool(raw_config_options["json"]))

    except (RuntimeError, FireboltError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


@command()
@common_options
@option(
    "--name",
    help="Database name, that should be updated",
    required=True,
    type=str,
)
@option(
    "--description",
    help="Database textual description up to 64 characters",
    type=str,
    required=True,
)
@json_option
def update(**raw_config_options: str) -> None:
    """
    Update specified database description
    """
    try:
        rm = construct_resource_manager(**raw_config_options)
        database = rm.databases.get_by_name(name=raw_config_options["name"])

        database = database.update(description=raw_config_options["description"])

        if not raw_config_options["json"]:
            echo(f"The database {database.name} was successfully updated")

        print_db_full_information(rm, database, bool(raw_config_options["json"]))

    except (RuntimeError, FireboltError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


database.add_command(create)
database.add_command(list)
database.add_command(drop)
database.add_command(describe)
database.add_command(update)
