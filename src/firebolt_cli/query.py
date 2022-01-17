import csv
import os
import sys
from typing import Optional

import click
from click import command, echo, option
from firebolt.common.exception import FireboltError
from firebolt.db.connection import connect
from tabulate import tabulate

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
)


def read_from_file(fpath: Optional[str]) -> str:
    """
    read from file, if fpath is not None, otherwise return empty string
    """
    if fpath is None:
        return ""
    else:
        with open(fpath, "r") as f:
            return f.read()


def read_from_stdin_buffer() -> str:
    """
    read from buffer if stdin file descriptor is open, otherwise return empty string
    """
    if not sys.stdin.isatty():
        return sys.stdin.buffer.read().decode("utf-8")
    else:
        return ""


@command()
@common_options
@option("--engine-name")
@option("--engine-url")
@option("--csv", is_flag=True)
@option("--database-name", callback=default_from_config_file())
@option(
    "--sql",
    help="path to the file with the sql query to be executed",
    type=click.Path(exists=True),
)
def query(**raw_config_options: str) -> None:
    """
    Execute sql queries
    """
    stdin_query = read_from_stdin_buffer()
    file_query = read_from_file(raw_config_options["sql"])

    if stdin_query == "" and file_query == "":
        echo(
            "SQL Query should be provided either from file or from stdin;"
            "The interactive SQL is not implemented yet",
            err=True,
        )
        sys.exit(os.EX_SOFTWARE)

    if stdin_query != "" and file_query != "":
        echo(
            "SQL request should be either read from stdin or file, both are specified",
            err=True,
        )
        sys.exit(os.EX_USAGE)

    sql_query = stdin_query if stdin_query != "" else file_query

    try:
        connection = connect(
            engine_url=raw_config_options["engine_url"],
            engine_name=raw_config_options["engine_name"],
            database=raw_config_options["database_name"],
            username=raw_config_options["username"],
            password=raw_config_options["password"],
            api_endpoint=raw_config_options["api_endpoint"],
        )
    except FireboltError as err:
        echo(err, err=True)
        sys.exit(os.EX_UNAVAILABLE)

    try:
        cursor = connection.cursor()

        cursor.execute(sql_query)
        data = cursor.fetchall()

        headers = [i.name for i in cursor.description]
        if raw_config_options["csv"]:
            writer = csv.writer(sys.stdout)
            writer.writerow(headers)
            writer.writerows(data)
        else:
            echo(tabulate(data, headers=headers, tablefmt="grid"))

    except FireboltError as err:
        echo(err, err=True)
        sys.exit(os.EX_UNAVAILABLE)
    finally:
        connection.close()
