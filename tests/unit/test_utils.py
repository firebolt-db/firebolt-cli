import json
import os
from collections import namedtuple
from typing import Dict, Sequence
from unittest import mock
from unittest.mock import ANY

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from firebolt.client.auth import ClientCredentials
from firebolt.common import Settings
from firebolt.common.exception import FireboltError
from firebolt.service.manager import ResourceManager
from httpx import HTTPStatusError
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.main import main
from firebolt_cli.utils import (
    config_file,
    construct_resource_manager,
    convert_bytes,
    create_aws_creds_from_environ,
    create_connection,
    format_short_statement,
    get_default_database_engine,
    prepare_execution_result_line,
    prepare_execution_result_table,
    read_config,
    to_human_readable,
    update_config,
)


def test_prepare_execution_empty() -> None:
    headers = ["name0", "name1", "name2", "name3"]
    assert json.loads(prepare_execution_result_table([], headers, use_json=True)) == []
    assert len(prepare_execution_result_table([], headers, use_json=False)) > 0


def test_prepare_execution_single() -> None:
    data = [0, 1, 2, 3]
    headers = ["name0", "name1", "name2", "name3"]

    j = json.loads(prepare_execution_result_line(data, headers, use_json=True))
    for header in headers:
        assert header in j

    assert (
        len(prepare_execution_result_line(data, headers, use_json=False).split("\n"))
        > 0
    )


def test_prepare_execution_multiple() -> None:
    data = [[0, 1, 2, 3], [4, 5, 6, 7]]
    headers = ["name0", "name1", "name2", "name3"]

    j = json.loads(prepare_execution_result_table(data, headers, use_json=True))
    assert len(j) == 2

    for header in headers:
        assert header in j[0]
        assert header in j[1]

    assert (
        len(prepare_execution_result_table(data, headers, use_json=False).split("\n"))
        > 0
    )


def test_prepare_execution_wrong_header() -> None:
    data = [[0, 1, 2, 3], [4, 5, 6, 7]]
    headers = ["name0", "name1", "name2", "name3"]
    wrong_data = [[0, 1, 2, 3], [4, 5, 6]]
    wrong_headers = ["name0", "name1", "name2"]

    with pytest.raises(ValueError):
        prepare_execution_result_table(wrong_data, headers, use_json=True)

    with pytest.raises(ValueError):
        prepare_execution_result_table(data, wrong_headers, use_json=True)

    with pytest.raises(ValueError):
        prepare_execution_result_line(wrong_data, headers, use_json=False)

    with pytest.raises(ValueError):
        prepare_execution_result_line(data, wrong_headers, use_json=False)


def test_convert_bytes() -> None:
    assert "" == convert_bytes(None)

    with pytest.raises(ValueError):
        convert_bytes(-10.0)

    assert "0 KB" == convert_bytes(0)
    assert "1 KB" == convert_bytes(2**10)
    assert "1 MB" == convert_bytes(2**20)
    assert "1 GB" == convert_bytes(2**30)
    assert "1.2 GB" == convert_bytes(1.2 * 2**30)
    assert "9.99 GB" == convert_bytes(9.99 * 2**30)
    assert "19.99 EB" == convert_bytes(19.99 * 2**60)


def test_to_human_readable() -> None:
    """
    more cases for testing converting to human-readable
    """
    assert "0.5 x2" == to_human_readable(1, 2, ["x2", "x4", "x8"])
    assert "1.5 x2" == to_human_readable(3, 2, ["x2", "x4", "x8"])
    assert "1 x4" == to_human_readable(4, 2, ["x2", "x4", "x8"])
    assert "2 x8" == to_human_readable(16, 2, ["x2", "x4", "x8"])
    assert "32 x8" == to_human_readable(256, 2, ["x2", "x4", "x8"])


ALL_CONFIG_PARAMS = [
    "username",
    "account_name",
    "api_endpoint",
    "engine_name",
    "database_name",
    "password",
]


def test_config_get_set_single(fs: FakeFilesystem) -> None:
    """
    Test get/set/delete _password commands
    """
    fs.create_dir(user_config_dir())

    # set all to zero
    update_config(**dict(zip(ALL_CONFIG_PARAMS, [""] * len(ALL_CONFIG_PARAMS))))

    for param in ALL_CONFIG_PARAMS:
        value = f"{param}_secret_value"
        update_config(**dict({(param, value)}))

        config = read_config()
        assert param in config
        assert config[param] == value
        assert len(config) == 1

        update_config(**dict({(param, "")}))


def test_config_get_set_all(fs: FakeFilesystem) -> None:
    """
    Test get/set/delete _password commands
    """
    fs.create_dir(user_config_dir())

    # set all to zero
    update_config(**dict(zip(ALL_CONFIG_PARAMS, [""] * len(ALL_CONFIG_PARAMS))))
    update_config(
        username="username_value",
        account_name="account_name_value",
        api_endpoint="api_endpoint_value",
        engine_name="engine_name_value",
        database_name="database_name_value",
        password="password_value",
    )

    config = read_config()
    assert len(config) == 6
    assert config["username"] == "username_value"
    assert config["account_name"] == "account_name_value"
    assert config["api_endpoint"] == "api_endpoint_value"
    assert config["engine_name"] == "engine_name_value"
    assert config["database_name"] == "database_name_value"
    assert config["password"] == "password_value"


def test_config_caching(fs: FakeFilesystem) -> None:
    """
    Check that read_config function actually uses cached information, and returns
    the same config even after config file was deleted
    """
    fs.create_dir(user_config_dir())
    update_config(username="username_value")

    old_config = read_config()
    fs.remove(config_file)
    new_config = read_config()
    assert old_config == new_config


def test_construct_resource_manager(mocker: MockerFixture):
    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    sa_mock = mocker.patch.object(ClientCredentials, "__init__", return_value=None)
    settings_mock = mocker.patch.object(Settings, "__init__", return_value=None)

    construct_resource_manager(
        client_id="client_id",
        client_secret="client_secret",
        api_endpoint="endpoint.firebolt.io",
        account_name="firebolt",
        access_token=None,
    )
    rm.assert_called_once()
    sa_mock.assert_called_once_with("client_id", "client_secret")
    settings_mock.assert_called_once_with(
        auth=ANY,
        account_name="firebolt",
        server="endpoint.firebolt.io",
        default_region="",
    )


def test_database_get_default_engine_happy_path(
    configure_resource_manager: Sequence, mocker: MockerFixture
):
    rm, database, engine = configure_resource_manager

    _Engine = namedtuple("Engine", "engine_id is_default_engine")
    rm.bindings.get_many.return_value = [
        _Engine(11, False),
        _Engine(12, True),
        _Engine(13, False),
    ]

    get_default_database_engine(ResourceManager(), "database_name")

    rm.engines.get.assert_called_once_with(12)
    rm.databases.get_by_name.assert_called_once_with(name="database_name")


def test_database_get_default_engine_empty(
    configure_resource_manager: Sequence, mocker: MockerFixture
):
    rm, database, engine = configure_resource_manager

    namedtuple("Engine", "engine_id is_default_engine")
    rm.bindings.get_many.return_value = []

    with pytest.raises(FireboltError):
        get_default_database_engine(ResourceManager(), "database_name")

    rm.databases.get_by_name.assert_called_once_with(name="database_name")


def test_database_get_default_engine_none(
    configure_resource_manager: Sequence, mocker: MockerFixture
):
    rm, database, engine = configure_resource_manager

    _Engine = namedtuple("Engine", "engine_id is_default_engine")
    rm.bindings.get_many.return_value = [
        _Engine(11, False),
        _Engine(12, False),
        _Engine(13, False),
    ]

    with pytest.raises(FireboltError):
        get_default_database_engine(ResourceManager(), "database_name")

    rm.databases.get_by_name.assert_called_once_with(name="database_name")


def test_create_connection(
    mock_connection_params: dict, mocker: MockerFixture
):
    """
    Check create_connection with engine name and access_token
    """
    connect_function_mock = mocker.patch("firebolt_cli.utils.connect")
    sa_mock = mocker.patch.object(ClientCredentials, "__init__", return_value=None)

    create_connection(**mock_connection_params)

    connect_function_mock.assert_called_once_with(
        auth=ANY,
        account_name=mock_connection_params["account_name"],
        api_endpoint=mock_connection_params["api_endpoint"],
        database=mock_connection_params["database_name"],
        engine_name=mock_connection_params["engine_name"],
    )

    sa_mock.assert_called_once_with(
        mock_connection_params["client_id"],
        mock_connection_params["client_secret"],
    )

def test_create_aws_creds_from_environ_happy_path():
    """
    Test correct cases of construction of aws_creds
    """
    with mock.patch.dict(
        os.environ,
        {
            "FIREBOLT_AWS_KEY_ID": "mock_key_id",
            "FIREBOLT_AWS_SECRET_KEY": "mock_secret",
        },
    ):
        aws_creds = create_aws_creds_from_environ()

        assert aws_creds.key_secret_creds.aws_key_id == "mock_key_id"
        assert (
            aws_creds.key_secret_creds.aws_secret_key.get_secret_value()
            == "mock_secret"
        )

    with mock.patch.dict(
        os.environ,
        {
            "FIREBOLT_AWS_ROLE_ARN": "mock_role_arn",
            "FIREBOLT_AWS_ROLE_EXTERNAL_ID": "mock_external_id",
        },
    ):
        aws_creds = create_aws_creds_from_environ()

        assert aws_creds.role_creds.role_arn.get_secret_value() == "mock_role_arn"
        assert aws_creds.role_creds.external_id == "mock_external_id"

    with mock.patch.dict(os.environ, {"FIREBOLT_AWS_ROLE_ARN": "mock_role_arn"}):
        aws_creds = create_aws_creds_from_environ()

        assert aws_creds.role_creds.role_arn.get_secret_value() == "mock_role_arn"
        assert aws_creds.role_creds.external_id is None

    with mock.patch.dict(os.environ, {}):
        assert create_aws_creds_from_environ() is None


def test_create_aws_creds_from_environ_invalide():
    """
    Test incorrect cases of construction of aws_creds
    """
    with mock.patch.dict(os.environ, {"FIREBOLT_AWS_KEY_ID": "mock_value"}):
        with pytest.raises(FireboltError):
            create_aws_creds_from_environ()

    with mock.patch.dict(os.environ, {"FIREBOLT_AWS_SECRET_KEY": "mock_value"}):
        with pytest.raises(FireboltError):
            create_aws_creds_from_environ()

    with mock.patch.dict(os.environ, {"FIREBOLT_AWS_ROLE_EXTERNAL_ID": "mock_value"}):
        with pytest.raises(FireboltError):
            create_aws_creds_from_environ()

    with mock.patch.dict(
        os.environ,
        {
            "FIREBOLT_AWS_KEY_ID": "mock_value",
            "FIREBOLT_AWS_SECRET_KEY": "mock_value",
            "FIREBOLT_AWS_ROLE_ARN": "mock_value",
        },
    ):
        with pytest.raises(FireboltError):
            create_aws_creds_from_environ()

    with mock.patch.dict(
        os.environ,
        {
            "FIREBOLT_AWS_SECRET_KEY": "mock_value",
            "FIREBOLT_AWS_ROLE_EXTERNAL_ID": "mock_value",
        },
    ):
        with pytest.raises(FireboltError):
            create_aws_creds_from_environ()


def test_main_incorrect_command():
    """
    test calling non_existing_command should result into an error and a help message
    """
    result = CliRunner().invoke(main, ["non_existing_command"])
    assert result.exit_code != 0

    assert "Usage:" in result.stdout
    assert "Error: No such command" in result.stdout


@pytest.mark.parametrize(
    "argument, expected",
    [
        ("SELECT 1", "SELECT 1"),
        ("/**/ SELECT 1", "SELECT 1"),
        ("-- SELECT 1;\nSELECT 2", "SELECT 2"),
        ("SELECT        23          \n FROM table\n", "SELECT 23 FROM table"),
    ],
)
def test_format_short_statement(argument: str, expected: str):
    """
    test common cases of format_short_statement
    """
    assert format_short_statement(argument) == expected


def test_format_short_statement_truncate():
    """
    test format_short_statement with truncate_long_string paramter
    """
    assert format_short_statement("SELECT 123", truncate_long_string=6) == "SELECT ..."
