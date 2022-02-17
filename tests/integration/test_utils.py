from firebolt.service.manager import ResourceManager

from firebolt_cli.utils import (
    construct_resource_manager,
    read_config,
    update_config,
)


def test_construct_resource_manager(
    api_endpoint: str,
    account_name: str,
    password: str,
    username: str,
):
    # Token is not provided, should use username/password for connection
    update_config(token="")
    rm = construct_resource_manager(
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )

    assert isinstance(
        rm, ResourceManager
    ), "failed to construct resource manager without a token"
    assert len(read_config().get("token", "")) != 0, "token is empty"

    # Token is provided, should use it for connection
    rm = construct_resource_manager(api_endpoint=api_endpoint)
    assert isinstance(
        rm, ResourceManager
    ), "failed to construct resource manager with a token"


def test_construct_resource_manager_invalid_token(
    api_endpoint: str,
    account_name: str,
    password: str,
    username: str,
):
    # Invalid token is not provided, should use username/password for connection
    update_config(token="invalid_token")
    rm = construct_resource_manager(
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )

    assert isinstance(
        rm, ResourceManager
    ), "failed to construct resource manager using username/passoword"
    new_token = read_config().get("token", "")
    assert len(new_token) != 0, "token is empty"
    assert new_token != "invalid_token", "token is not updated"
