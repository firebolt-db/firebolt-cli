import os
import sys
from datetime import timedelta
from typing import Callable, Optional

from click import (
    Choice,
    IntRange,
    argument,
    command,
    confirm,
    echo,
    group,
    option,
)
from firebolt.common.exception import FireboltError
from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt.service.types import (
    EngineStatus,
    EngineType,
    WarmupMethod,
)

from firebolt_cli.common_options import (
    common_options,
    default_from_config_file,
    json_option,
)
from firebolt_cli.utils import (
    construct_resource_manager,
    construct_shortcuts,
    convert_bytes,
    convert_price_per_hour,
    exit_on_firebolt_exception,
    prepare_execution_result_line,
    prepare_execution_result_table,
)


@group(
    cls=construct_shortcuts(
        shortages={
            "list": "list (ls)",
            "ls": "list (ls)",
        }
    )
)
def engine() -> None:
    """
    Manage engines.
    """


def start_stop_generic(
    engine: Engine,
    action: str,
    accepted_initial_states: set,
    accepted_final_states: set,
    wrong_initial_state_error: str,
    failure_message: str,
    success_message: str,
) -> None:

    if engine.current_status not in accepted_initial_states:
        raise FireboltError(
            wrong_initial_state_error.format(
                name=engine.name,
                state=engine.current_status.name,
            )
        )


    if action == "start":
        engine = engine.start()
    elif action == "stop":
        engine = engine.stop()
    elif action == "restart":
        engine = engine.stop()
        engine = engine.start()
    else:
        assert False, "not available action"

    if engine.current_status in accepted_final_states:
        echo(success_message.format(name=engine.name))
    else:
        raise FireboltError(
            failure_message.format(name=engine.name, status=engine.current_status.name)
        )


@command()
@common_options
@argument(
    "engine_name",
    type=str,
    required=False,
    callback=default_from_config_file(required=True)
)
@exit_on_firebolt_exception
def start(**raw_config_options: str) -> None:
    """
    Start an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])

    if engine.current_status in (EngineStatus.STARTED, EngineStatus.RUNNING):
        echo(f"Engine {engine.name} is already running")
        return

    start_stop_generic(
        engine=engine,
        action="start",
        accepted_initial_states={
            EngineStatus.STOPPED,
            EngineStatus.STOPPING,
            EngineStatus.STARTING,
        },
        accepted_final_states={EngineStatus.RUNNING},
        wrong_initial_state_error="Engine {name} is not in a stopped state."
        "The current engine state is {state}.",
        success_message="Engine {name} was successfully started.",
        failure_message="Engine {name} failed to start. Engine status: {status}."
    )


@command()
@common_options
@argument(
    "engine_name",
    type=str,
    required=False,
    callback=default_from_config_file(required=True)
)
@exit_on_firebolt_exception
def stop(**raw_config_options: str) -> None:
    """
    Stop an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])

    if engine.current_status == EngineStatus.STOPPED:
        echo(f"Engine {engine.name} is already stopped")
        return

    start_stop_generic(
        engine=engine,
        action="stop",
        accepted_initial_states={
            EngineStatus.RUNNING,
            EngineStatus.STARTED,
            EngineStatus.STARTING,
            EngineStatus.STOPPING,
        },
        accepted_final_states={EngineStatus.STOPPED, EngineStatus.STOPPING},
        wrong_initial_state_error="Engine {name} is not in a "
        "running or initializing state. The current engine state is {state}.",
        success_message="Engine {name} was successfully stopped.",
        failure_message="Engine {name} failed to stop. Engine status: {status}.",
    )


def engine_properties_options(create_mode: bool = True) -> Callable:
    """
    decorator for engine create/update common options

    :param create_mode: True for create, will make some options required
    """
    _ENGINE_OPTIONS = [
        option(
            "--name",
            help="Name of the engine.",
            type=str,
            required=True,
        ),
        option(
            "--spec",
            help="Engine spec. Run 'firebolt engine get-instance-types' "
            "to get a list of available spec",
            type=str,
            required=False,
        ),
        option(
            "--type",
            help='Engine type: "rw" for general purpose '
            'and "ro" for data analytics.',
            type=Choice(list(ENGINE_TYPES.keys()), case_sensitive=False),
            default="ro" if create_mode else None,
            required=False,
        ),
        option(
            "--scale",
            help="The number of engine nodes. Value entered must be between 1 and 128.",
            type=IntRange(1, 128, clamp=False),
            default=1 if create_mode else None,
            required=False,
            show_default=True,
            metavar="INTEGER",
        ),
        option(
            "--auto-stop",
            help="Stop engine automatically after specified time in minutes."
            "Value entered must be between 0 and 43200"
            "(max value is equal to 30 days). "
            "Setting auto-stop to zero disables auto-stop.",
            type=IntRange(0, 30 * 24 * 60, clamp=False),
            default=20 if create_mode else None,
            required=False,
            show_default=True,
            metavar="INTEGER",
        ),
        option(
            "--warmup",
            help="Engine warmup method. "
            "Minimal(min), Preload indexes(ind), Preload all data(all)",
            type=Choice(list(WARMUP_METHODS.keys())),
            default="ind" if create_mode else None,
            required=False,
            show_default=True,
        ),
    ]

    def _engine_properties_options_inner(command: Callable) -> Callable:
        for add_option in reversed(_ENGINE_OPTIONS):
            command = add_option(command)
        return command

    return _engine_properties_options_inner


def echo_engine_information(
    rm: ResourceManager, engine: Engine, use_json: bool
) -> None:
    """

    :param engine:
    :param database:
    :param use_json:
    :return:
    """

    def _format_auto_stop(auto_stop: int) -> str:
        """
        auto_stop could be set either 0 or to a integer seconds value
        we print its timedelta or "ALWAYS ON"
        """
        return str(timedelta(seconds=auto_stop)) if auto_stop else "ALWAYS ON"

    def to_display(camel_case: str) -> str:
        return camel_case.lower().replace("_", " ").title()

    echo(
        prepare_execution_result_line(
            data=[
                engine.name,
                engine.current_status.name if engine.current_status else "-",
                _format_auto_stop(engine.auto_stop),
                to_display(engine.type.name),
                to_display(engine.warmup.name),
                engine._database_name,
                engine.spec.name if engine.spec else "",
                engine.scale,
            ],
            header=[
                "name",
                "status",
                "auto_stop",
                "type",
                "warm_up",
                "attached_to_database",
                "instance_type",
                "scale",
            ],
            use_json=bool(use_json),
        )
    )


ENGINE_TYPES = {"rw": EngineType.GENERAL_PURPOSE, "ro": EngineType.DATA_ANALYTICS}
WARMUP_METHODS = {
    "min": WarmupMethod.MINIMAL,
    "ind": WarmupMethod.PRELOAD_INDEXES,
    "all": WarmupMethod.PRELOAD_ALL_DATA,
}


@command()
@common_options
@argument(
    "engine_name",
    type=str,
    required=False,
    callback=default_from_config_file(required=True)
)
@exit_on_firebolt_exception
def restart(**raw_config_options: str) -> None:
    """
    Restart an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])

    start_stop_generic(
        engine=engine,
        action="restart",
        accepted_initial_states={
            EngineStatus.RUNNING,
            EngineStatus.STOPPED,
            EngineStatus.STOPPING,
            EngineStatus.STARTED,
            EngineStatus.STARTING,
            EngineStatus.FAILED,
        },
        accepted_final_states={EngineStatus.RUNNING},
        wrong_initial_state_error="Engine {name} is not in a running or failed state."
        " The current engine state is {state}.",
        success_message="Engine {name} was successfully restarted.",
        failure_message="Engine {name} failed to restart. Engine status: {status}.",
    )


@command()
@common_options
@engine_properties_options(create_mode=True)
@option(
    "--database-name",
    help="Name of the database the engine should be attached to.",
    type=str,
    required=True,
)
@json_option
@exit_on_firebolt_exception
def create(**raw_config_options: str) -> None:
    """
    Creates engine with the requested parameters.
    """
    rm = construct_resource_manager(**raw_config_options)

    database = rm.databases.get(raw_config_options["database_name"])
    engine = rm.engines.create(
        name=raw_config_options["name"],
        spec=raw_config_options["spec"],
        region=database.region,
        engine_type=ENGINE_TYPES[raw_config_options["type"]],
        scale=int(raw_config_options["scale"]),
        auto_stop=int(raw_config_options["auto_stop"]),
        warmup=WARMUP_METHODS[raw_config_options["warmup"]],
    )

    try:
        engine.attach_to_database(database)
    except (FireboltError, RuntimeError) as err:
        engine.delete()
        raise err

    if not raw_config_options["json"]:
        echo(
            f"Engine {engine.name} was successfully created "
            f"and attached to the {database.name}."
        )

    echo_engine_information(rm, engine, bool(raw_config_options["json"]))


@command()
@common_options
@engine_properties_options(create_mode=False)
@option(
    "--new-engine-name",
    help="Set this parameter for renaming the engine.",
    default=None,
    required=False,
)
@json_option
@exit_on_firebolt_exception
def update(
    auto_stop: int, scale: int, **raw_config_options: str
) -> None:
    """
    Update engine parameters. Engine should be stopped before updating.
    """
    something_to_update = (
        any(
            raw_config_options[param] is not None
            for param in [
                "spec",
                "warmup",
                "type",
            ]
        )
        or scale is not None
        or auto_stop is not None
    )

    if not something_to_update:
        echo("Nothing to update. At least one parameter should be provided.", err=True)
        sys.exit(os.EX_USAGE)

    rm = construct_resource_manager(**raw_config_options)

    engine = rm.engines.get(raw_config_options["name"])

    engine = engine.update(
        name=raw_config_options["new_engine_name"],
        spec=raw_config_options["spec"],
        engine_type=ENGINE_TYPES.get(raw_config_options["type"], None),
        scale=scale,
        auto_stop=auto_stop,
        warmup=WARMUP_METHODS.get(raw_config_options["warmup"], None),
    )

    if not raw_config_options["json"]:
        echo(f"Engine {engine.name} was successfully updated.")

    echo_engine_information(rm, engine, bool(raw_config_options["json"]))


@command()
@common_options
@argument(
    "engine_name",
    type=str,
    required=False,
    callback=default_from_config_file(required=True)
)
@exit_on_firebolt_exception
def status(**raw_config_options: str) -> None:
    """
    Check the ENGINE_NAME status. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])

    current_status_name = (
        engine.current_status.name if engine.current_status else "-"
    )
    echo(f"Engine {engine.name} current status is: {current_status_name}")


@command(name="list", short_help="List existing engines (alias: ls)")
@common_options
@option(
    "--name-contains",
    help="A string used to filter the list of returned engines. "
    "Partial matches will be returned.",
    default=None,
    type=str,
)
@option(
    "--database",
    help="Only list engines attached to this database.",
    default=None,
    type=str,
)
@option(
    "--region",
    help="Only list engines in this region",
    default=None,
    type=str,
)
@option(
    "--current-status",
    help="Only list engines with this status",
    default=None,
    type=Choice([e.name.lower() for e in EngineStatus], case_sensitive=False),
)
@option(
    "--current-status-not",
    help="Omit engines with this status",
    default=None,
    type=Choice([e.name.lower() for e in EngineStatus], case_sensitive=False),
)
@json_option
@exit_on_firebolt_exception
def list(**raw_config_options: str) -> None:
    """
    List existing engines
    """

    rm = construct_resource_manager(**raw_config_options)

    current_status_eq = (
        raw_config_options["current_status"].lower().capitalize()
        if raw_config_options["current_status"] else None
    )
    current_status_not_eq = (
        raw_config_options["current_status_not"].lower().capitalize()
        if raw_config_options["current_status_not"] else None
    )

    engines = rm.engines.get_many(
        name_contains=raw_config_options["name_contains"],
        database_name=raw_config_options["database"],
        current_status_eq=current_status_eq,
        current_status_not_eq=current_status_not_eq,
        region_eq=raw_config_options["region"]
    )

    if not raw_config_options["json"]:
        echo("Found {num_engines} engines".format(num_engines=len(engines)))

    if raw_config_options["json"] or engines:
        echo(
            prepare_execution_result_table(
                data=[
                    [
                        engine.name,
                        engine.current_status.name if engine.current_status else "-",
                        engine.region,
                    ]
                    for engine in engines
                ],
                header=["name", "status", "region"],
                use_json=bool(raw_config_options["json"]),
            )
        )


@command()
@common_options
@option(
    "--yes",
    help="Automatic yes on confirmation prompt",
    is_flag=True,
)
@argument(
    "engine_name",
    type=str,
)
@exit_on_firebolt_exception
def drop(**raw_config_options: str) -> None:
    """
    Drop an existing engine
    """
    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])

    if raw_config_options["yes"] or confirm(
        f"Do you really want to drop the engine {engine.name}?"
    ):
        engine.delete()
        echo(f"Engine {engine.name} was successfully dropped")
    else:
        echo("Drop request is aborted")


@command()
@common_options
@argument(
    "engine_name",
    type=str,
    required=False,
    callback=default_from_config_file(required=True)
)
@json_option
@exit_on_firebolt_exception
def describe(**raw_config_options: str) -> None:
    """
    Describe specified engine
    """
    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get(raw_config_options["engine_name"])
    echo_engine_information(rm, engine, bool(raw_config_options["json"]))


@command()
@common_options
@json_option
@exit_on_firebolt_exception
def get_instance_types(**raw_config_options: str) -> None:
    """
    Get instance types (spec) available for your account
    """
    rm = construct_resource_manager(**raw_config_options)

    echo(
        prepare_execution_result_table(
            data=[
                [
                    spec.name,
                    spec.cpu_virtual_cores_count,
                    convert_bytes(int(spec.memory_size_bytes)),
                    convert_bytes(int(spec.storage_size_bytes)),
                    convert_price_per_hour(spec.price_per_hour_cents),
                ]
                for spec in sorted(
                    rm.instance_types.instance_types,
                    key=lambda x: (x.name[0], int(x.cpu_virtual_cores_count)),
                )
            ],
            header=["name", "cpu", "memory", "storage", "price"],
            use_json=bool(raw_config_options["json"]),
        )
    )


engine.add_command(get_instance_types)
engine.add_command(create)
engine.add_command(describe)
engine.add_command(drop)
engine.add_command(start)
engine.add_command(restart)
engine.add_command(stop)
engine.add_command(status)
engine.add_command(update)
engine.add_command(list)
