import os
import sys

from click import Choice, IntRange, command, echo, group, option
from firebolt.common.exception import FireboltError
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)

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


def start_stop_generic(
    action: str,
    accepted_initial_states: set,
    accepted_final_states: set,
    accepted_final_nowait_states: set,
    wrong_initial_state_error: str,
    failure_message: str,
    success_message: str,
    success_message_nowait: str,
    **raw_config_options: str,
) -> None:

    try:
        rm = construct_resource_manager(**raw_config_options)

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
            engine = engine.stop(wait_for_stop=not raw_config_options["nowait"])
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
            raise FireboltError(
                failure_message.format(
                    name=engine.name, status=engine.current_status_summary
                )
            )

    except (FireboltError, RuntimeError) as err:
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
        failure_message="Engine {name} failed to start. Engine status: {status}.",
        **raw_config_options,
    )


@command()
@common_options
@option(
    "--name",
    help="Name of the engine, engine should be in running or starting state",
    type=str,
    required=True,
)
@option(
    "--nowait",
    help="If the flag is set, the command will finish"
    " immediately after sending the stop request",
    is_flag=True,
)
def stop(**raw_config_options: str) -> None:
    """
    Stop an existing engine
    """

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
        failure_message="Engine {name} failed to stop. Engine status: {status}.",
        **raw_config_options,
    )


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
    help="Engine spec",
    type=Choice(
        ["C{}".format(i) for i in range(1, 8)]
        + ["S{}".format(i) for i in range(1, 7)]
        + ["B{}".format(i) for i in range(1, 8)]
        + ["M{}".format(i) for i in range(1, 8)],
        case_sensitive=False,
    ),
    required=True,
)
@option(
    "--description",
    help="Engine description",
    type=str,
    default="",
    required=False,
)
@option(
    "--type",
    help="Engine type: rw for general purpose and ro for data analytics",
    type=Choice(["ro", "rw"], case_sensitive=False),
    default="ro",
    required=False,
)
@option(
    "--scale",
    help="Engine scale",
    type=IntRange(1, 128, clamp=False),
    default=1,
    required=False,
    show_default=True,
)
@option(
    "--auto_stop",
    help="Stop engine automatically after specified time in minutes",
    type=IntRange(1, 30 * 24 * 60, clamp=False),
    default=20,
    required=False,
    show_default=True,
)
@option(
    "--warmup",
    help="Engine warmup method. "
    "Minimal(min), Preload indexes(ind), Preload all data(all) ",
    type=Choice(["min", "ind", "all"]),
    default="ind",
    required=False,
    show_default=True,
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

    ENGINE_TYPES = {"rw": EngineType.GENERAL_PURPOSE, "ro": EngineType.DATA_ANALYTICS}
    WARMUP_METHODS = {
        "min": WarmupMethod.MINIMAL,
        "ind": WarmupMethod.PRELOAD_INDEXES,
        "all": WarmupMethod.PRELOAD_ALL_DATA,
    }

    rm = construct_resource_manager(**raw_config_options)

    try:
        database = rm.databases.get_by_name(raw_config_options["database_name"])

        engine = rm.engines.create(
            name=raw_config_options["name"],
            spec=raw_config_options["spec"],
            region=raw_config_options["region"],
            engine_type=ENGINE_TYPES[raw_config_options["type"]],
            scale=int(raw_config_options["scale"]),
            auto_stop=int(raw_config_options["auto_stop"]),
            warmup=WARMUP_METHODS[raw_config_options["warmup"]],
            description=raw_config_options["description"],
        )

        try:
            database.attach_to_engine(engine, is_default_engine=True)
        except (FireboltError, RuntimeError) as err:
            engine.delete()
            raise err

    except (FireboltError, RuntimeError) as err:
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
                engine.settings.is_read_only,
                engine.settings.auto_stop_delay_duration,
                engine.settings.preset,
                engine.settings.warm_up,
                str(engine.create_time),
                database.name,
            ],
            header=[
                "name",
                "description",
                "is_read_only",
                "auto_stop",
                "preset",
                "warm_up",
                "create_time",
                "attached_to_database",
            ],
            use_json=bool(raw_config_options["json"]),
        )
    )


@command()
@common_options
@option(
    "--name",
    help="Name of the engine",
    type=str,
    required=True,
)
def status(**raw_config_options: str) -> None:
    """
    Check the engine status
    """

    rm = construct_resource_manager(**raw_config_options)
    try:
        engine = rm.engines.get_by_name(name=raw_config_options["name"])

        echo(f"Engine {engine.name} current status is: {engine.current_status_summary}")
    except (FireboltError, RuntimeError) as err:
        echo(err, err=True)
        sys.exit(os.EX_DATAERR)


engine.add_command(start)
engine.add_command(create)
engine.add_command(stop)
engine.add_command(status)
