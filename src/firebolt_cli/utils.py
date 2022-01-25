import json
from typing import Sequence

from firebolt.common import Settings
from firebolt.service.manager import ResourceManager
from tabulate import tabulate


def prepare_execution_result_line(
    data: Sequence, header: Sequence, use_json: bool = False
) -> str:
    """
    return the string representation of data in either json or tabular formats.
    In case of json, the result is dict
    In case of tabular, the result is table with headers in the first column
    """

    if len(data) != len(header):
        raise ValueError("data and header have different length")

    if use_json:
        return json.dumps(dict(zip(header, data)), indent=4)
    else:
        return tabulate(list(zip(header, data)), tablefmt="grid")


def prepare_execution_result_table(
    data: Sequence[Sequence], header: Sequence, use_json: bool = False
) -> str:
    """
    return the string representation of data in either json or tabular formats
    In case of json, the result is list of dicts
    In case of tabular, the result is table with headers in the first row
    """
    for d in data:
        if len(d) != len(header):
            raise ValueError("data and header have different length")

    if use_json:
        return json.dumps([dict(zip(header, d)) for d in data], indent=4)
    else:
        return tabulate(data, headers=header, tablefmt="grid")


def construct_resource_manager(**raw_config_options: str) -> ResourceManager:
    """
    Propagate raw_config_options to the settings and construct a resource manager
    """

    settings = Settings(
        server=raw_config_options["api_endpoint"],
        user=raw_config_options["username"],
        password=raw_config_options["password"],
        default_region=raw_config_options.get("region", ""),
    )

    return ResourceManager(settings)
