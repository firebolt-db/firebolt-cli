import os
import sys

from click import command, echo, group, option
from firebolt.common.exception import FireboltError
from firebolt.service.types import EngineStatusSummary

from firebolt_cli.common_options import common_options
from firebolt_cli.utils import construct_resource_manager


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


engine.add_command(start)
