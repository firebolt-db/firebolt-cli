import os
import sys
from typing import Any, Dict

from click import Choice, command, echo, option
from firebolt.common.exception import FireboltError
from firebolt_ingest.table_service import TableService

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
@option(
    "--mode",
    help="Mode of ingestion. "
    "Overwrite (default) - will drop and fully recreate fact table."
    "Append - will only insert files, that aren't in the fact table yet.",
    required=False,
    default="overwrite",
    type=Choice(["append", "overwrite"], case_sensitive=False),
)
@exit_on_firebolt_exception
def ingest(**raw_config_options: str) -> None:
    """
    [Beta] Ingest the data from external to fact table.
    """

    with create_connection(**raw_config_options) as connection:
        ts = TableService(connection)

        params: Dict[str, Any] = {
            "internal_table_name": raw_config_options["fact_table_name"],
            "external_table_name": raw_config_options["external_table_name"],
            "firebolt_dont_wait_for_upload_to_s3": False,
        }
        if raw_config_options["mode"] == "overwrite":
            ts.insert_full_overwrite(**params)
        elif raw_config_options["mode"] == "append":
            ts.insert_incremental_append(**params)
        else:
            raise FireboltError(f"Mode: {raw_config_options['mode']} unknown.")

        echo(
            f"Ingestion from '{raw_config_options['external_table_name']}' "
            f"to '{raw_config_options['fact_table_name']}' was successful."
        )

        if not ts.verify_ingestion(
            external_table_name=raw_config_options["external_table_name"],
            internal_table_name=raw_config_options["fact_table_name"],
        ):
            echo(
                "WARNING: Nevertheless some discrepancy between fact and "
                "external table were found. It is recommended to do the full "
                "overwrite of the fact table.",
                err=True,
            )
            sys.exit(os.EX_DATAERR)
