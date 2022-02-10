import csv
import os
import sys

import click
from click import command, echo, option
from firebolt.common.exception import FireboltError
from firebolt.db import Cursor
from firebolt.db.connection import connect
from prompt_toolkit.application import get_app
from prompt_toolkit.enums import DEFAULT_BUFFER
from prompt_toolkit.filters import Condition
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.shortcuts import PromptSession
from pygments.lexers import PostgresLexer
from tabulate import tabulate

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
    option_engine_name_url,
)
from firebolt_cli.utils import read_from_file, read_from_stdin_buffer


def print_result_if_any(cursor: Cursor, use_csv: bool) -> None:
    """
    Fetch the data from cursor and print it in csv or tabular format
    """
    while 1:
        if cursor.description:
            data = cursor.fetchall()

            headers = [i.name for i in cursor.description]
            if use_csv:
                writer = csv.writer(sys.stdout)
                writer.writerow(headers)
                writer.writerows(data)
            else:
                echo(tabulate(data, headers=headers, tablefmt="grid"))

        if not cursor.nextset():
            break


@Condition
def is_multilne_needed() -> bool:
    """
    function reads the buffer of the interactive prompt
    and return true if the continuation of the request is required
    """
    buffer = get_app().layout.get_buffer_by_name(DEFAULT_BUFFER)
    if buffer is None:
        return True

    text = buffer.text.strip()
    return len(text) != 0 and not text.endswith(";")


def enter_interactive_session(cursor: Cursor, use_csv: bool) -> None:
    """
    Enters an infinite loop of interactive shell
    """
    echo("Connection succeeded")

    session: PromptSession = PromptSession(
        message="firebolt> ",
        prompt_continuation="     ...> ",
        lexer=PygmentsLexer(PostgresLexer),
        multiline=is_multilne_needed,
    )

    while 1:
        try:
            sql_query = session.prompt()
            sql_query = sql_query.strip().rstrip(";")
            if len(sql_query) == 0:
                continue

            cursor.execute(sql_query)
            print_result_if_any(cursor, use_csv=use_csv)
        except FireboltError as err:
            echo(err)
            continue
        except KeyboardInterrupt:
            continue
        except EOFError:
            echo("Bye!")
            break


@command()
@common_options
@option_engine_name_url(read_from_config=True)
@option("--csv", help="Provide query output in csv format", is_flag=True, default=False)
@option(
    "--database-name",
    help="Database name to use for SQL queries",
    callback=default_from_config_file(),
)
@option(
    "--file",
    help="Path to the file with the sql query to be executed",
    default=None,
    type=click.Path(exists=True),
)
def query(**raw_config_options: str) -> None:
    """
    Execute sql queries
    """
    stdin_query = read_from_stdin_buffer()
    file_query = read_from_file(raw_config_options["file"])

    if stdin_query and file_query:
        echo(
            "SQL request should be either read from stdin or file, both are specified",
            err=True,
        )
        sys.exit(os.EX_USAGE)

    sql_query = stdin_query or file_query

    try:
        with connect(
            engine_url=raw_config_options["engine_url"],
            engine_name=raw_config_options["engine_name"],
            database=raw_config_options["database_name"],
            username=raw_config_options["username"],
            password=raw_config_options["password"],
            api_endpoint=raw_config_options["api_endpoint"],
        ) as connection:

            cursor = connection.cursor()

            if sql_query:
                # if query is available, then execute, print result and exit
                cursor.execute(sql_query)
                print_result_if_any(cursor, bool(raw_config_options["csv"]))
            else:
                # otherwise start the interactive session
                enter_interactive_session(cursor, bool(raw_config_options["csv"]))

    except FireboltError as err:
        echo(err, err=True)
        sys.exit(os.EX_UNAVAILABLE)
