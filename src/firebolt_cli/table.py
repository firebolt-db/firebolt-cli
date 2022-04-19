import click
from click import command, echo, group, option
from firebolt.db.connection import connect
from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Table
from firebolt_ingest.service import TableService

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
)
from firebolt_cli.utils import (
    exit_on_firebolt_exception,
    extract_engine_name_url,
    read_from_file,
)


@group(name="table (tb)")
def table() -> None:
    """
    Creating tables
    """


@command()
@common_options
@option(
    "--engine-name",
    help="Name or url of the engine to use for SQL queries",
    envvar="FIREBOLT_ENGINE_NAME",
    callback=default_from_config_file(required=True),
)
@option(
    "--database-name",
    help="Database name to use for SQL queries",
    envvar="FIREBOLT_DATABASE_NAME",
    callback=default_from_config_file(required=True),
)
@option(
    "--s3-url",
    help="Path to the s3 bucket, where the data is stored",
    required=True,
)
@option(
    "--file",
    help="Path to the yaml config file",
    type=click.Path(exists=True),
    required=True,
)
@exit_on_firebolt_exception
def create_external(**raw_config_options: str) -> None:
    """
    Create external table
    """
    aws_settings = AWSSettings(s3_url=raw_config_options["s3_url"])
    table = Table.parse_yaml(read_from_file(raw_config_options["file"]))
    engine_name, engine_url = extract_engine_name_url(raw_config_options["engine_name"])

    with connect(
        engine_url=engine_url,
        engine_name=engine_name,
        database=raw_config_options["database_name"],
        username=raw_config_options["username"],
        password=raw_config_options["password"],
        api_endpoint=raw_config_options["api_endpoint"],
        account_name=raw_config_options["account_name"],
    ) as connection:
        TableService(connection, aws_settings).create_external_table(table)
        echo(f"External table ({table.table_name}) was successfully created")


@command()
@common_options
@option(
    "--engine-name",
    help="Name or url of the engine to use for SQL queries",
    envvar="FIREBOLT_ENGINE_NAME",
    callback=default_from_config_file(required=False),
)
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Database name to use for SQL queries",
    callback=default_from_config_file(required=True),
)
@option(
    "--file",
    help="Path to the yaml config file",
    type=click.Path(exists=True),
    required=True,
)
@exit_on_firebolt_exception
def create_fact(**raw_config_options: str) -> None:
    """
    Create fact table
    """
    table_yaml_string = read_from_file(raw_config_options["file"])
    table = Table.parse_yaml(table_yaml_string)

    engine_name, engine_url = extract_engine_name_url(raw_config_options["engine_name"])

    with connect(
        engine_url=engine_url,
        engine_name=engine_name,
        database=raw_config_options["database_name"],
        username=raw_config_options["username"],
        password=raw_config_options["password"],
        api_endpoint=raw_config_options["api_endpoint"],
        account_name=raw_config_options["account_name"],
    ) as connection:
        TableService(connection).create_internal_table(table)
        echo(f"Fact table ({table.table_name}) was successfully created")


table.add_command(create_external)
table.add_command(create_fact)