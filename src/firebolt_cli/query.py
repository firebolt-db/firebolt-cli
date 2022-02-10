import csv
import os
import sys
from typing import Optional

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
    read_config_key,
)

EXIT_COMMANDS = [".exit", ".quit", ".q"]
HELP_COMMANDS = [".help"]
INTERNAL_COMMANDS = EXIT_COMMANDS + HELP_COMMANDS + [".tables"]


def read_from_file(fpath: Optional[str]) -> Optional[str]:
    """
    read from file, if fpath is not None, otherwise return empty string
    """
    if fpath is None:
        return None

    with open(fpath, "r") as f:
        return f.read() or None


def read_from_stdin_buffer() -> Optional[str]:
    """
    read from buffer if stdin file descriptor is open, otherwise return empty string
    """
    if sys.stdin.isatty():
        return None

    return sys.stdin.buffer.read().decode("utf-8") or None


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
    return not (len(text) == 0 or text.endswith(";") or text in INTERNAL_COMMANDS)


def show_help() -> None:
    """
    Print help message with internal commands of interactive sql execution
    """
    rows = [
        [".exit", "Exit firebolt-cli"],
        [".help", "Show this help message"],
        [".quit", "Exit firebolt-cli"],
        [".tables", "Show tables in current database"],
    ]

    for internal_command, help_message in rows:
        echo("{:<10s}".format(internal_command), nl=False)
        echo(help_message)


def process_internal_command(internal_command: str) -> str:
    """
    process internal command, execute an internal command
    or make an sql query from internal_command
    """
    if internal_command in EXIT_COMMANDS:
        raise EOFError()
    elif internal_command in HELP_COMMANDS:
        show_help()
        return ""
    elif internal_command == ".tables":
        return "SHOW tables;"

    raise ValueError(f"Not known internal command: {internal_command}")


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

            if sql_query in INTERNAL_COMMANDS:
                sql_query = process_internal_command(sql_query)

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
@option_engine_name_url
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
    if (
        raw_config_options["engine_name"] is None
        and raw_config_options["engine_url"] is None
    ):
        raw_config_options["engine_name"] = read_config_key("engine_name")
        raw_config_options["engine_url"] = read_config_key("engine_url")

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
