import os
import sys

from click import command, echo, group, option
from firebolt.common import Settings
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from firebolt.service.types import EngineStatusSummary

from firebolt_cli.common_options import common_options


@group()
def engine() -> None:
    """
    Manage the engines using the firebolt cli
    """


def start_stop_generic(
    action: str,
    accepted_initial_states: set,
    accepted_final_states: set,
    accepted_final_nowait_states: set,
    wrong_initial_state_error: str,
    failure_message: str,
    success_message: str,
    success_message_nowait: str,
    **raw_config_options: str
) -> None:

    settings = Settings(
        server=raw_config_options["api_endpoint"],
        user=raw_config_options["username"],
        password=raw_config_options["password"],
        default_region="",
    )

    try:
        rm = ResourceManager(settings=settings)

        engine = rm.engines.get_by_name(raw_config_options["name"])
        if engine.current_status_summary not in accepted_initial_states:

            raise FireboltError(
                wrong_initial_state_error.format(
                    name=engine.name,
                    state=engine.current_status_summary,
                )
            )

        if action == "start":
            engine = engine.start(wait_for_startup=not raw_config_options["nowait"])
        elif action == "stop":
            engine = engine.stop()
        else:
            assert False, "not available action"

        if (
            engine.current_status_summary in accepted_final_nowait_states
            and raw_config_options["nowait"]
        ):
            echo(success_message_nowait.format(name=engine.name))
        elif engine.current_status_summary in accepted_final_states:
            echo(success_message.format(name=engine.name))
        else:
            raise FireboltError(failure_message.format(name=engine.name))

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
    "--nowait",
    help="If the flag is set, the command will finish"
    " immediately after sending the start request",
    is_flag=True,
)
def start(**raw_config_options: str) -> None:
    """
    Start an existing engine
    """
    start_stop_generic(
        action="start",
        accepted_initial_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        },
        accepted_final_states={EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING},
        accepted_final_nowait_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING
        },
        wrong_initial_state_error="Engine {name} is not in a stopped state,"
        " the current engine state is {state}",
        success_message="Engine {name} is successfully started",
        success_message_nowait="Start request for engine {name} is successfully sent",
        failure_message="Engine {name} failed to start",
        **raw_config_options
    )


@command()
@common_options
@option(
    "--name",
    help="Name of the engine, engine should be in running or starting state",
    type=str,
    required=True,
)
def stop(**raw_config_options: str) -> None:
    """
    Stop an existing engine
    """

    raw_config_options["nowait"] = None
    start_stop_generic(
        action="stop",
        accepted_initial_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        },
        accepted_final_states={EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED},
        accepted_final_nowait_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        },
        wrong_initial_state_error="Engine {name} is not in a running or starting state,"
        " the current engine state is {state}",
        success_message="Engine {name} is successfully stopped",
        success_message_nowait="Stop request for engine {name} is successfully sent",
        failure_message="Engine {name} failed to stop",
        **raw_config_options
    )


engine.add_command(start)
engine.add_command(stop)
