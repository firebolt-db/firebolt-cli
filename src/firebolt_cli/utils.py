import json

from tabulate import tabulate


def prepare_execution_result(data: dict, use_json: bool = False) -> str:
    """
    return the string representation of data in either json or tabular formats
    """
    if use_json:
        return json.dumps(data, indent=4)
    else:
        return tabulate(data.items(), tablefmt="grid")
