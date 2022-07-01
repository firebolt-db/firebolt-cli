import os
import sys
from typing import Dict

import click
from click import Choice, command, echo, option
from firebolt.common.exception import FireboltError
from firebolt_ingest.table_model import Table
from firebolt_ingest.table_service import TableService

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
)
from firebolt_cli.utils import (
    create_connection,
    exit_on_firebolt_exception,
    read_from_file,
)


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
    "--file",
    help="Path to the yaml config file",
    type=click.Path(exists=True),
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
@option(
    "--firebolt_dont_wait_for_upload_to_s3",
    help="Don't wait for upload part to S3 on insert query finish. ",
    is_flag=True,
    required=False,
    default=False,
)
@option(
    "--advanced_mode",
    help="execute set advanced_mode=1",
    is_flag=True,
    required=False,
    default=False,
)
@option(
    "--use_short_column_path_parquet",
    help="Use short parquet column path "
    "(skipping repeated nodes and their child node).",
    is_flag=True,
    required=False,
    default=False,
)
@exit_on_firebolt_exception
def ingest(**raw_config_options: str) -> None:
    """
    [Beta] Ingest the data from external to fact table.
    """

    table = Table.parse_yaml(read_from_file(raw_config_options["file"]))

    with create_connection(**raw_config_options) as connection:
        ts = TableService(table, connection)

        params: Dict[str, bool] = {
            "firebolt_dont_wait_for_upload_to_s3": bool(
                raw_config_options["firebolt_dont_wait_for_upload_to_s3"]
            ),
            "advanced_mode": bool(raw_config_options["advanced_mode"]),
            "use_short_column_path_parquet": bool(
                raw_config_options["use_short_column_path_parquet"]
            ),
        }

        if raw_config_options["mode"] == "overwrite":
            ts.insert_full_overwrite(**params)
        elif raw_config_options["mode"] == "append":
            ts.insert_incremental_append(**params)
        else:
            raise FireboltError(f"Mode: {raw_config_options['mode']} unknown.")

        echo(
            f"Ingestion from 'ex_{table.table_name}' "
            f"to '{table.table_name}' was successful."
        )

        if not ts.verify_ingestion():
            echo(
                "WARNING: Nevertheless some discrepancy between fact and "
                "external table were found. It is recommended to do the full "
                "overwrite of the fact table.",
                err=True,
            )
            sys.exit(os.EX_DATAERR)
