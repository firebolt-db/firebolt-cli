import os
import sys

from click import Choice, command, echo, group, option
from firebolt.common.exception import FireboltError
from firebolt.service.types import EngineStatusSummary

from firebolt_cli.common_options import common_options
from firebolt_cli.utils import (
    construct_resource_manager,
    prepare_execution_result_line,
)


@group()
def engine() -> None:
    """
    Manage the engines using the firebolt cli
    """


@command()
@common_options
@option(
    "--name",
    help="Name of the engine, engine should be in stopped state",
    type=str,
    required=True,
)
@option(
    "--nowait",
    help="If the flag is set, the command will finish"
    " immediately after sending the start request",
    is_flag=True,
)
def start(**raw_config_options: str) -> None:
    """
    Start an existing engine
    """
    try:
        rm = construct_resource_manager(**raw_config_options)

        engine = rm.engines.get_by_name(raw_config_options["name"])
        if engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:

            raise FireboltError(
                "Engine {name} is not in a stopped state, "
                "the current engine state is {state}".format(
                    name=engine.name,
                    state=engine.current_status_summary,
                )
            )

        engine = engine.start(wait_for_startup=not raw_config_options["nowait"])

        if (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING
            and raw_config_options["nowait"]
        ):
            echo(
                "Start request for engine {name} is successfully sent".format(
                    name=engine.name
                )
            )
        elif (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
        ):
            echo("Engine {name} is successfully started".format(name=engine.name))
        else:
            raise FireboltError(
                "Engine {name} failed to start".format(name=engine.name)
            )

    except FireboltError as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


@command()
@common_options
@option(
    "--name",
    help="Name of the engine, engine should be in stopped state",
    type=str,
    required=True,
)
@option(
    "--database_name",
    help="Name of the database the engine should be attached to",
    type=str,
    required=True,
)
@option(
    "--spec",
    help="Name of the engine, engine should be in stopped state",
    type=Choice(
        ["C{}".format(i) for i in range(1, 8)]
        + ["S{}".format(i) for i in range(1, 7)]
        + ["B{}".format(i) for i in range(1, 8)]
        + ["M{}".format(i) for i in range(1, 8)],
        case_sensitive=False,
    ),
    required=True,
)
@option("--region", required=True)
@option(
    "--json",
    is_flag=True,
    help="Provide output in json format",
)
def create(**raw_config_options: str) -> None:
    """
    Creates engine with the requested parameters
    """
    rm = construct_resource_manager(**raw_config_options)

    try:
        database = rm.databases.get_by_name(raw_config_options["database_name"])

        engine = rm.engines.create(
            name=raw_config_options["name"],
            spec=raw_config_options["spec"],
            region="us-east-1",
        )
        try:
            database.attach_to_engine(engine, is_default_engine=True)
        except FireboltError as err:
            engine.delete()
            raise err

    except FireboltError as err:
        echo(err, err=True)
        sys.exit(os.EX_USAGE)

    if not raw_config_options["json"]:
        echo(
            f"Engine {engine.name} is successfully created"
            " and attached to the {database.name}"
        )

    echo(
        prepare_execution_result_line(
            data=[
                engine.name,
                engine.description,
                str(engine.create_time),
                database.name,
            ],
            header=["name", "description", "create_time", "attached_to_database"],
            use_json=bool(raw_config_options["json"]),
        )
    )


engine.add_command(start)
engine.add_command(create)
