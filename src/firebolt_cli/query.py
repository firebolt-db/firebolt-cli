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
)
from firebolt_cli.utils import (
    construct_resource_manager,
    exit_on_firebolt_exception,
    get_default_database_engine,
    read_from_file,
    read_from_stdin_buffer,
)

EXIT_COMMANDS = [".exit", ".quit", ".q"]
HELP_COMMANDS = [".help", ".h"]
TABLES_COMMAND = ".tables"
INTERNAL_COMMANDS = EXIT_COMMANDS + HELP_COMMANDS + [TABLES_COMMAND]


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
        ["/".join(HELP_COMMANDS), "Show this help message"],
        ["/".join(EXIT_COMMANDS), "Exit firebolt-cli"],
        [TABLES_COMMAND, "Show tables in current database"],
    ]

    for internal_command, help_message in rows:
        echo("{:<15s}".format(internal_command), nl=False)
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
    elif internal_command == TABLES_COMMAND:
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
@option(
    "--engine-name",
    help="Name or url of the engine to use for SQL queries",
    envvar="FIREBOLT_ENGINE_NAME",
    callback=default_from_config_file(required=False),
)
@option("--csv", help="Provide query output in csv format", is_flag=True, default=False)
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Database name to use for SQL queries",
    callback=default_from_config_file(),
)
@option(
    "--file",
    help="Path to the file with the sql query to be executed",
    default=None,
    type=click.Path(exists=True),
)
@exit_on_firebolt_exception
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

    # Decide whether to store the value as engine_name or engine_url
    # '.' symbol should always be in url and cannot be in engine_name
    engine_name, engine_url = None, None

    if raw_config_options["engine_name"] is None:
        rm = construct_resource_manager(**raw_config_options)
        engine_name = get_default_database_engine(
            rm, raw_config_options["database_name"]
        ).name
    else:
        if "." in raw_config_options["engine_name"]:
            engine_url = raw_config_options["engine_name"]
        else:
            engine_name = raw_config_options["engine_name"]

    with connect(
        engine_url=engine_url,
        engine_name=engine_name,
        database=raw_config_options["database_name"],
        username=raw_config_options["username"],
        password=raw_config_options["password"],
        api_endpoint=raw_config_options["api_endpoint"],
        account_name=raw_config_options["account_name"],
    ) as connection:

        cursor = connection.cursor()

        if sql_query:
            # if query is available, then execute, print result and exit
            cursor.execute(sql_query)
            print_result_if_any(cursor, bool(raw_config_options["csv"]))
        else:
            # otherwise start the interactive session
            enter_interactive_session(cursor, bool(raw_config_options["csv"]))
