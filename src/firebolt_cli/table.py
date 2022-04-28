import click
from click import command, echo, group, option
from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.table_model import Table
from firebolt_ingest.table_service import TableService

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
)
from firebolt_cli.utils import (
    create_aws_creds_from_environ,
    create_connection,
    exit_on_firebolt_exception,
    read_from_file,
)


@group(name="table", short_help="Create tables (alias: tb)")
def table() -> None:
    """
    Create tables.
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
    aws_settings = AWSSettings(
        s3_url=raw_config_options["s3_url"],
        aws_credentials=create_aws_creds_from_environ(),
    )

    table = Table.parse_yaml(read_from_file(raw_config_options["file"]))

    with create_connection(**raw_config_options) as connection:
        TableService(connection).create_external_table(table, aws_settings)
        echo(f"External table (ex_{table.table_name}) was successfully created")


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
@option(
    "--add-file-metadata",
    help="Add meta columns (source_file_name and source_file_timestamp)"
    " to the fact table",
    is_flag=True,
    default=False,
)
@exit_on_firebolt_exception
def create_fact(**raw_config_options: str) -> None:
    """
    Create fact table
    """
    table = Table.parse_yaml(read_from_file(raw_config_options["file"]))

    with create_connection(**raw_config_options) as connection:
        TableService(connection).create_internal_table(
            table=table, add_file_metadata=raw_config_options["add_file_metadata"]
        )
        echo(f"Fact table ({table.table_name}) was successfully created")


table.add_command(create_external)
table.add_command(create_fact)
