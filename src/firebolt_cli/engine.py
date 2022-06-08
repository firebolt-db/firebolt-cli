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
    EngineStatusSummary,
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
    exit_on_firebolt_exception,
    get_default_database_engine,
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


def get_engine_from_name_or_default(
    rm: ResourceManager, engine_name: Optional[str], database_name: Optional[str]
) -> Engine:
    """
    Returns engine either from its name, or a default engine deducted
    from database_name. At least one engine_name or database_name should
    be provided, raises an Error otherwise.
    """
    if engine_name is not None:
        return rm.engines.get_by_name(name=engine_name)
    elif database_name is not None:
        return get_default_database_engine(rm, database_name)
    else:
        raise FireboltError("Either engine name or database name has to be specified")


def start_stop_generic(
    engine: Engine,
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

    current_status_name = (
        engine.current_status_summary.name
        if engine.current_status_summary
        else EngineStatusSummary.ENGINE_STATUS_SUMMARY_UNSPECIFIED.name
    )

    if engine.current_status_summary not in accepted_initial_states:
        raise FireboltError(
            wrong_initial_state_error.format(
                name=engine.name,
                state=current_status_name,
            )
        )

    if action == "start":
        engine = engine.start(wait_for_startup=raw_config_options["wait"])
    elif action == "stop":
        engine = engine.stop(wait_for_stop=raw_config_options["wait"])
    elif action == "restart":
        engine = engine.restart(wait_for_startup=raw_config_options["wait"])
    else:
        assert False, "not available action"

    if (
        engine.current_status_summary in accepted_final_nowait_states
        and not raw_config_options["wait"]
    ):
        echo(success_message_nowait.format(name=engine.name))
    elif engine.current_status_summary in accepted_final_states:
        echo(success_message.format(name=engine.name))
    else:
        raise FireboltError(
            failure_message.format(name=engine.name, status=current_status_name)
        )


@command()
@common_options
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Alternatively to engine name, database name could be specified, "
    "its default engine will be used",
    hidden=True,
    callback=default_from_config_file(required=False),
)
@option(
    "--wait/--no-wait",
    help="Wait until the engine is started.",
    is_flag=True,
    default=False,
)
@argument("engine_name", type=str, required=False)
@exit_on_firebolt_exception
def start(**raw_config_options: str) -> None:
    """
    Start an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = get_engine_from_name_or_default(
        rm, raw_config_options["engine_name"], raw_config_options["database_name"]
    )

    if (
        engine.current_status_summary
        == EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED
    ):
        raise FireboltError(
            f"Engine {engine.name} is in a failed state.\n"
            f"You need to restart an engine first:\n"
            f"$ firebolt restart {engine.name}"
        )

    start_stop_generic(
        engine=engine,
        action="start",
        accepted_initial_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
        },
        accepted_final_states={EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING},
        accepted_final_nowait_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING
        },
        wrong_initial_state_error="Engine {name} is not in a stopped state."
        "The current engine state is {state}.",
        success_message="Engine {name} is successfully started.",
        success_message_nowait="Start request for engine {name} is successfully sent.",
        failure_message="Engine {name} failed to start. Engine status: {status}.",
        **raw_config_options,
    )


@command()
@common_options
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Alternatively to engine name, database name could be specified, "
    "its default engine will be used",
    hidden=True,
    callback=default_from_config_file(required=False),
)
@option(
    "--wait/--no-wait",
    help="Wait until the engine is stopped.",
    is_flag=True,
    default=False,
)
@argument("engine_name", type=str, required=False)
@exit_on_firebolt_exception
def stop(**raw_config_options: str) -> None:
    """
    Stop an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = get_engine_from_name_or_default(
        rm, raw_config_options["engine_name"], raw_config_options["database_name"]
    )

    start_stop_generic(
        engine=engine,
        action="stop",
        accepted_initial_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING,
        },
        accepted_final_states={EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED},
        accepted_final_nowait_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
        },
        wrong_initial_state_error="Engine {name} is not in a "
        "running or initializing state. The current engine state is {state}.",
        success_message="Engine {name} is successfully stopped.",
        success_message_nowait="Stop request for engine {name} is successfully sent.",
        failure_message="Engine {name} failed to stop. Engine status: {status}.",
        **raw_config_options,
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
            required=create_mode,
        ),
        option(
            "--description",
            help="Engine description (max: 64 characters).",
            type=str,
            default="" if create_mode else None,
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
            "--use-spot/--no-use-spot",
            help="Use spot instances",
            is_flag=True,
            default=None,
            required=False,
        ),
        option(
            "--auto-stop",
            help="Stop engine automatically after specified time in minutes."
            "Value entered must be between 1 and 43200"
            "(max value is equal to 30 days).",
            type=IntRange(1, 30 * 24 * 60, clamp=False),
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

    revision = None
    instance_type = None
    if engine.latest_revision_key:
        revision = rm.engine_revisions.get_by_key(engine.latest_revision_key)
        instance_type = rm.instance_types.instance_types_by_key[
            revision.specification.db_compute_instances_type_key
        ]

    def _format_auto_stop(auto_stop: str) -> str:
        """
        auto_stop could be set either 0 or to a value with ending with m or s
        if it is the case then we print its timedelta or "ALWAYS ON"
        if not the original auto_stop parameter is returned
        """
        val = int(auto_stop[:-1])

        if val == 0:
            return "ALWAYS ON"

        if auto_stop[-1] == "m":
            return str(timedelta(minutes=val))
        elif auto_stop[-1] == "s":
            return str(timedelta(seconds=val))
        else:
            return auto_stop

    echo(
        prepare_execution_result_line(
            data=[
                engine.name,
                engine.description,
                engine.current_status_summary.name
                if engine.current_status_summary
                else None,
                _format_auto_stop(engine.settings.auto_stop_delay_duration),
                revision.specification.db_compute_instances_use_spot
                if revision
                else "",
                engine.settings.preset,
                engine.settings.warm_up,
                str(engine.create_time),
                engine.database.name if engine.database else None,
                instance_type.name if instance_type else "",
                revision.specification.db_compute_instances_count if revision else "",
            ],
            header=[
                "name",
                "description",
                "status",
                "auto_stop",
                "is_spot_instance",
                "preset",
                "warm_up",
                "create_time",
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
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Alternatively to engine name, database name could be specified, "
    "its default engine will be used",
    hidden=True,
    callback=default_from_config_file(required=False),
)
@option(
    "--wait/--no-wait",
    help="Wait until the engine is restarted.",
    is_flag=True,
    default=False,
)
@argument("engine_name", type=str, required=False)
@exit_on_firebolt_exception
def restart(**raw_config_options: str) -> None:
    """
    Restart an existing ENGINE_NAME. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = get_engine_from_name_or_default(
        rm, raw_config_options["engine_name"], raw_config_options["database_name"]
    )

    start_stop_generic(
        engine=engine,
        action="restart",
        accepted_initial_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        },
        accepted_final_states={EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING},
        accepted_final_nowait_states={
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
        },
        wrong_initial_state_error="Engine {name} is not in a running or failed state."
        " The current engine state is {state}.",
        success_message="Engine {name} is successfully restarted.",
        success_message_nowait="Restart request for engine {name} "
        "is successfully sent.",
        failure_message="Engine {name} failed to restart. Engine status: {status}.",
        **raw_config_options,
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

    database = rm.databases.get_by_name(name=raw_config_options["database_name"])
    region = rm.regions.get_by_key(database.compute_region_key)

    engine = rm.engines.create(
        name=raw_config_options["name"],
        spec=raw_config_options["spec"],
        region=region.name,
        engine_type=ENGINE_TYPES[raw_config_options["type"]],
        scale=int(raw_config_options["scale"]),
        auto_stop=int(raw_config_options["auto_stop"]),
        warmup=WARMUP_METHODS[raw_config_options["warmup"]],
        description=raw_config_options["description"],
        revision_spec_kwargs={
            "db_compute_instances_use_spot": True
            if raw_config_options["use_spot"]
            else False
        },
    )

    try:
        database.attach_to_engine(engine=engine, is_default_engine=True)
    except (FireboltError, RuntimeError) as err:
        engine.delete()
        raise err

    if not raw_config_options["json"]:
        echo(
            f"Engine {engine.name} is successfully created "
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
    use_spot: Optional[bool], auto_stop: int, scale: int, **raw_config_options: str
) -> None:
    """
    Update engine parameters. Engine should be stopped before updating.
    """
    something_to_update = (
        any(
            raw_config_options[param] is not None
            for param in [
                "spec",
                "type",
                "warmup",
                "description",
            ]
        )
        or scale is not None
        or use_spot is not None
        or auto_stop is not None
    )

    if not something_to_update:
        echo("Nothing to update. At least one parameter should be provided.", err=True)
        sys.exit(os.EX_USAGE)

    rm = construct_resource_manager(**raw_config_options)

    engine = rm.engines.get_by_name(name=raw_config_options["name"])

    engine = engine.update(
        name=raw_config_options["new_engine_name"],
        spec=raw_config_options["spec"],
        engine_type=ENGINE_TYPES.get(raw_config_options["type"], None),
        scale=scale,
        auto_stop=auto_stop,
        warmup=WARMUP_METHODS.get(raw_config_options["warmup"], None),
        description=raw_config_options["description"],
        use_spot=use_spot,
    )

    if not raw_config_options["json"]:
        echo(f"Engine {engine.name} is successfully updated.")

    echo_engine_information(rm, engine, bool(raw_config_options["json"]))


@command()
@common_options
@option(
    "--database-name",
    envvar="FIREBOLT_DATABASE_NAME",
    help="Alternatively to engine name, database name could be specified, "
    "its default engine will be used",
    hidden=True,
    callback=default_from_config_file(required=False),
)
@argument("engine_name", type=str, required=False)
@exit_on_firebolt_exception
def status(**raw_config_options: str) -> None:
    """
    Check the ENGINE_NAME status. If ENGINE_NAME is not set,
    uses default engine instead.
    """

    rm = construct_resource_manager(**raw_config_options)
    engine = get_engine_from_name_or_default(
        rm, raw_config_options["engine_name"], raw_config_options["database_name"]
    )

    current_status_name = (
        engine.current_status_summary.name if engine.current_status_summary else ""
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
@json_option
@exit_on_firebolt_exception
def list(**raw_config_options: str) -> None:
    """
    List existing engines
    """

    rm = construct_resource_manager(**raw_config_options)

    engines = rm.engines.get_many(
        name_contains=raw_config_options["name_contains"],
        order_by="ENGINE_ORDER_NAME_ASC",
    )

    if not raw_config_options["json"]:
        echo("Found {num_engines} engines".format(num_engines=len(engines)))

    if raw_config_options["json"] or engines:
        echo(
            prepare_execution_result_table(
                data=[
                    [
                        engine.name,
                        engine.current_status_summary.name
                        if engine.current_status_summary
                        else EngineStatusSummary.ENGINE_STATUS_SUMMARY_UNSPECIFIED,
                        rm.regions.get_by_key(engine.compute_region_key).name,
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
    engine = rm.engines.get_by_name(name=raw_config_options["engine_name"])

    if raw_config_options["yes"] or confirm(
        f"Do you really want to drop the engine {engine.name}?"
    ):
        engine.delete()
        echo(f"Drop request for engine {engine.name} is successfully sent")
    else:
        echo("Drop request is aborted")


@command()
@common_options
@argument(
    "engine_name",
    type=str,
)
@json_option
@exit_on_firebolt_exception
def describe(**raw_config_options: str) -> None:
    """
    Describe specified engine
    """
    rm = construct_resource_manager(**raw_config_options)
    engine = rm.engines.get_by_name(name=raw_config_options["engine_name"])
    echo_engine_information(rm, engine, bool(raw_config_options["json"]))


@command()
@common_options
@option(
    "--region",
    help="Instances information relevant to this region.",
    required=True,
    type=str,
)
@json_option
@exit_on_firebolt_exception
def get_instance_types(**raw_config_options: str) -> None:
    """
    Get instance types (spec) available for your account
    """
    rm = construct_resource_manager(**raw_config_options)
    if not raw_config_options["region"] in rm.regions.regions_by_name:
        raise FireboltError(
            f"Unknown region: {raw_config_options['region']}. "
            f"Available regions: {', '.join(rm.regions.regions_by_name.keys())}"
        )

    region = rm.regions.get_by_name(name=raw_config_options["region"])

    echo(
        prepare_execution_result_table(
            data=[
                [
                    spec.name,
                    spec.cpu_virtual_cores_count,
                    convert_bytes(spec.memory_size_bytes),
                    convert_bytes(spec.storage_size_bytes),
                ]
                for spec in sorted(
                    rm.instance_types.get_instance_types_per_region(region),
                    key=lambda x: (x.name[0], x.cpu_virtual_cores_count),
                )
            ],
            header=["name", "cpu", "memory", "storage"],
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
