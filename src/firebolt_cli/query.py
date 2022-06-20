import csv
import os
import sys
import time

import click
import sqlparse  # type: ignore
from click import command, echo, option
from firebolt.common.exception import FireboltError
from firebolt.db import Connection, Cursor
from prompt_toolkit.application import get_app
from prompt_toolkit.completion import DynamicCompleter, ThreadedCompleter
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
from firebolt_cli.completer import FireboltAutoCompleter
from firebolt_cli.utils import (
    construct_resource_manager,
    create_connection,
    exit_on_firebolt_exception,
    format_short_statement,
    get_default_database_engine,
    read_from_file,
    read_from_stdin_buffer,
)

EXIT_COMMANDS = [".exit", ".quit", ".q"]
HELP_COMMANDS = [".help", ".h"]
TABLES_COMMAND = ".tables"
INTERNAL_COMMANDS = EXIT_COMMANDS + HELP_COMMANDS + [TABLES_COMMAND]


def is_data_statement(statement: sqlparse.sql.Statement) -> bool:
    """
    true if the statement is supposed to output some data
    """
    token = statement.token_first(skip_cm=True, skip_ws=True).normalized
    return token in {"SHOW", "DESCRIBE", "EXPLAIN", "SELECT", "WITH"}


def format_time(execution_time: float) -> str:
    """
    format time from seconds into string in format: 99h 59m 59.99s
    """
    hours = int(execution_time // 3600)
    execution_time -= hours * 3600

    minutes = int(execution_time // 60)
    execution_time -= minutes * 60

    return (
        (f"{hours}h " if hours > 0 else "")
        + (f"{minutes}m " if minutes > 0 or hours > 0 else "")
        + f"{execution_time:.2f}s"
    )


def echo_execution_status(
    statement: str,
    statement_idx: int,
    statements_num: int,
    execution_time: float,
    success: bool,
) -> None:
    """
    print execution summary result: Success or Error
    and a shortened statement to which it is referring
    """
    statement = format_short_statement(statement)
    counter = "" if statements_num < 2 else f"({statement_idx}/{statements_num}) "
    formatted_time = format_time(execution_time)

    msg, color = (
        (f"{counter}Success ({formatted_time}):", "green")
        if success
        else (f"{counter}Error ({formatted_time}):", "yellow")
    )

    echo("{} {}".format(click.style(msg, fg=color, bold=True), statement))


def execute_and_print(cursor: Cursor, query: str, use_csv: bool) -> None:
    """
    Execute multiple queries one by one, fetch the data from cursor
    and print it in csv or tabular format.
    """
    statements = sqlparse.parse(query)
    for statement_idx, statement in enumerate(statements):
        try:
            start_time = time.time()
            cursor.execute(str(statement))
            execution_time = time.time() - start_time

            is_data = is_data_statement(statement)

            if not use_csv:
                echo_execution_status(
                    str(statement),
                    statement_idx + 1,
                    len(statements),
                    execution_time,
                    success=True,
                )

            if cursor.description and is_data:
                data = cursor.fetchall()

                headers = [i.name for i in cursor.description]
                if use_csv:
                    writer = csv.writer(sys.stdout)
                    writer.writerow(headers)
                    writer.writerows(data)
                else:
                    echo(tabulate(data, headers=headers, tablefmt="grid"))

            cursor.nextset()

        except FireboltError as err:
            execution_time = time.time() - start_time
            echo_execution_status(
                str(statement),
                statement_idx + 1,
                len(statements),
                execution_time,
                success=False,
            )
            raise err


@Condition
def is_multiline_needed() -> bool:
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
        ["/".join(HELP_COMMANDS), "Show this help message."],
        ["/".join(EXIT_COMMANDS), "Exit firebolt-cli."],
        [TABLES_COMMAND, "Show tables in current database."],
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


def enter_interactive_session(connection: Connection, use_csv: bool) -> None:
    """
    Enters an infinite loop of interactive shell.
    """
    echo("Connection succeeded.")

    completer = FireboltAutoCompleter(connection.cursor())

    session: PromptSession = PromptSession(
        message="firebolt> ",
        prompt_continuation="     ...> ",
        lexer=PygmentsLexer(PostgresLexer),
        completer=ThreadedCompleter(DynamicCompleter(lambda: completer)),
        multiline=is_multiline_needed,
    )

    cursor = connection.cursor()
    while 1:
        try:
            sql_query = session.prompt()
            sql_query = sql_query.strip().rstrip(";")

            if sql_query in INTERNAL_COMMANDS:
                sql_query = process_internal_command(sql_query)

            if len(sql_query) == 0:
                continue

            execute_and_print(cursor, sql_query, use_csv)
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
@option(
    "--csv", help="Provide query output in CSV format.", is_flag=True, default=False
)
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Database name to use for SQL queries.",
    callback=default_from_config_file(),
)
@option(
    "--file",
    help="Path to the file with the SQL query to be executed.",
    default=None,
    type=click.Path(exists=True),
)
@option("--sql", help="SQL statement, that will be executed", required=False)
@exit_on_firebolt_exception
def query(**raw_config_options: str) -> None:
    """
    Execute SQL queries.
    """
    stdin_query = read_from_stdin_buffer()
    file_query = read_from_file(raw_config_options["file"])
    args_query = raw_config_options["sql"]

    if bool(stdin_query) + bool(file_query) + bool(args_query) > 1:
        echo(
            "SQL request should be either read from stdin or file or "
            "command line arguments. Multiple are specified.",
            err=True,
        )
        sys.exit(os.EX_USAGE)

    sql_query = stdin_query or file_query or args_query

    # if engine_name is not set, use default engine
    if raw_config_options["engine_name"] is None:
        rm = construct_resource_manager(**raw_config_options)
        raw_config_options["engine_name"] = get_default_database_engine(
            rm, raw_config_options["database_name"]
        ).endpoint

    with create_connection(**raw_config_options) as connection:

        if sql_query:
            # if query is available, then execute, print result and exit
            execute_and_print(
                connection.cursor(), sql_query, bool(raw_config_options["csv"])
            )
        else:
            # otherwise start the interactive session
            enter_interactive_session(connection, bool(raw_config_options["csv"]))
