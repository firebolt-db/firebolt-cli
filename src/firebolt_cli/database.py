from click import argument, command, confirm, echo, group, option
from firebolt.model.database import Database
from firebolt.service.manager import ResourceManager

from firebolt_cli.common_options import common_options, json_option
from firebolt_cli.utils import (
    construct_resource_manager,
    construct_shortcuts,
    convert_bytes,
    exit_on_firebolt_exception,
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


@group(
    name="database (db)",
    cls=construct_shortcuts(
        shortages={
            "list": "list (ls)",
            "ls": "list (ls)",
        }
    ),
)
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
@option("--region", help="Region for the new database", required=True, type=str)
@exit_on_firebolt_exception
def create(**raw_config_options: str) -> None:
    """
    Create a new database
    """

    rm = construct_resource_manager(**raw_config_options)

    database = rm.databases.create(
        name=raw_config_options["name"],
        description=raw_config_options["description"],
        region=raw_config_options["region"],
    )

    if not raw_config_options["json"]:
        echo(f"Database {database.name} is successfully created")

    print_db_full_information(rm, database, bool(raw_config_options["json"]))


@command(name="list (ls)")
@common_options
@option(
    "--name-contains",
    help="Output databases will be filtered by name_contains",
    default=None,
    type=str,
)
@json_option
@exit_on_firebolt_exception
def list(**raw_config_options: str) -> None:
    """
    List existing databases
    """
    rm = construct_resource_manager(**raw_config_options)

    databases = rm.databases.get_many(
        name_contains=raw_config_options["name_contains"],
        order_by="DATABASE_ORDER_NAME_ASC",
    )

    if not raw_config_options["json"]:
        echo(f"Found {len(databases)} databases")

    if raw_config_options["json"] or databases:
        echo(
            prepare_execution_result_table(
                data=[
                    [
                        db.name,
                        str(rm.regions.get_by_key(db.compute_region_key).name),
                        db.description,
                    ]
                    for db in databases
                ],
                header=["name", "region", "description"],
                use_json=bool(raw_config_options["json"]),
            )
        )


@command()
@common_options
@option(
    "--yes",
    help="Automatic yes on confirmation prompt",
    is_flag=True,
)
@argument(
    "database_name",
    type=str,
)
@exit_on_firebolt_exception
def drop(**raw_config_options: str) -> None:
    """
    Drop specified database
    """
    rm = construct_resource_manager(**raw_config_options)
    database = rm.databases.get_by_name(name=raw_config_options["database_name"])

    if raw_config_options["yes"] or confirm(
        f"Do you really want to drop the database {database.name}?"
    ):
        database.delete()
        echo(f"Drop request for database {database.name} is successfully sent")
    else:
        echo("Drop request is aborted")


@command()
@common_options
@argument(
    "database_name",
    type=str,
)
@json_option
@exit_on_firebolt_exception
def describe(**raw_config_options: str) -> None:
    """
    Describe specified database
    """
    rm = construct_resource_manager(**raw_config_options)
    database = rm.databases.get_by_name(name=raw_config_options["database_name"])
    print_db_full_information(rm, database, bool(raw_config_options["json"]))


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
@exit_on_firebolt_exception
def update(**raw_config_options: str) -> None:
    """
    Update specified database description
    """
    rm = construct_resource_manager(**raw_config_options)
    database = rm.databases.get_by_name(name=raw_config_options["name"])

    database = database.update(description=raw_config_options["description"])

    if not raw_config_options["json"]:
        echo(f"The database {database.name} was successfully updated")

    print_db_full_information(rm, database, bool(raw_config_options["json"]))


database.add_command(create)
database.add_command(list)
database.add_command(drop)
database.add_command(describe)
database.add_command(update)
