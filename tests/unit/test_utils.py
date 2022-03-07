import json

import pytest
from appdirs import user_config_dir
from firebolt.common import Settings
from firebolt.service.manager import ResourceManager
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.utils import (
    construct_resource_manager,
    convert_bytes,
    prepare_execution_result_line,
    prepare_execution_result_table,
    read_config,
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
    assert "1 KB" == convert_bytes(2 ** 10)
    assert "1 MB" == convert_bytes(2 ** 20)
    assert "1 GB" == convert_bytes(2 ** 30)
    assert "1.2 GB" == convert_bytes(1.2 * 2 ** 30)
    assert "9.99 GB" == convert_bytes(9.99 * 2 ** 30)
    assert "19.99 EB" == convert_bytes(19.99 * 2 ** 60)


ALL_CONFIG_PARAMS = [
    "username",
    "account_name",
    "api_endpoint",
    "engine_name",
    "database_name",
    "token",
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


@pytest.mark.parametrize(
    "param", ["username", "password", "api_endpoint", "account_name"]
)
def test_config_token_invalidation(fs: FakeFilesystem, param: str) -> None:
    fs.create_dir(user_config_dir())
    update_config(token="token")

    update_config(**dict({(param, "value")}))
    config = read_config()

    assert "token" not in config, "token should have been deleted but it is not"
    assert param in config
    assert config[param] == "value"


def test_construct_resource_manager_password(mocker: MockerFixture):
    update_config(token="")

    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)
    client_mock = mocker.patch.object(ResourceManager, "client", create=True)
    client_mock.auth.token = "some_new_token"

    construct_resource_manager(
        username="username",
        password="password",
        api_endpoint="endpoint.firebolt.io",
        account_name="firebolt",
    )
    rm.assert_called_once_with(
        Settings(
            user="username",
            password="password",
            default_region="",
            account_name="firebolt",
            server="endpoint.firebolt.io",
        )
    )

    assert read_config().get("token") == "some_new_token"


def test_construct_resource_manager_token(mocker: MockerFixture):
    update_config(token="old_token")

    rm = mocker.patch.object(ResourceManager, "__init__", return_value=None)

    construct_resource_manager(
        username="username",
        password="password",
        api_endpoint="endpoint.firebolt.io",
        account_name="firebolt",
    )
    rm.assert_called_once_with(
        Settings(
            access_token="old_token",
            default_region="",
            server="endpoint.firebolt.io",
            account_name="firebolt",
        )
    )

    assert read_config().get("token") == "old_token"
