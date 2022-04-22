from click import command, echo, option
from firebolt_ingest.table_service import TableService  # type: ignore

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
)
from firebolt_cli.utils import create_connection, exit_on_firebolt_exception


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
    "--external-table-name",
    help="Name of external table from which the data will be fetched.",
    required=True,
)
@option(
    "--fact-table-name",
    help="Name of the fact table where the data will be ingested. "
    "The table must exist.",
    required=True,
)
@exit_on_firebolt_exception
def ingest(**raw_config_options: str) -> None:
    """
    Fully overwrite the data from external to fact table.
    """

    with create_connection(**raw_config_options) as connection:
        TableService(connection).insert_full_overwrite(
            internal_table_name=raw_config_options["fact_table_name"],
            external_table_name=raw_config_options["external_table_name"],
            firebolt_dont_wait_for_upload_to_s3=False,
        )

        echo(
            f"Ingestion from '{raw_config_options['external_table_name']}' "
            f"to '{raw_config_options['fact_table_name']}' was successful."
        )
