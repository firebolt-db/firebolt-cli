from logging import getLogger
from os import environ, makedirs

import pytest
from appdirs import user_config_dir
from click.testing import CliRunner
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import fixture

from firebolt_cli.configure import configure
from firebolt_cli.utils import construct_resource_manager
from firebolt.service.manager import ResourceManager

LOGGER = getLogger(__name__)

DATABASE_NAME_ENV = "DATABASE_NAME"
SERVICE_ID_ENV = "SERVICE_ID"
SERVICE_SECRET_ENV = "SERVICE_SECRET"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"
ENGINE_NAME_ENV = "ENGINE_NAME"
STOPPED_ENGINE_NAME_ENV = "STOPPED_ENGINE_NAME"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def stopped_engine_name() -> str:
    return must_env(STOPPED_ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def service_id() -> str:
    return must_env(SERVICE_ID_ENV)


@fixture(scope="session")
def service_secret() -> str:
    return must_env(SERVICE_SECRET_ENV)


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def default_region() -> str:
    return "us-east-1"


@fixture(scope="session")
def s3_url() -> str:
    return "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"


@fixture(autouse=True)
def configure_cli(
    cli_runner: CliRunner,
    api_endpoint: str,
    service_id: str,
    service_secret: str,
    database_name: str,
    engine_name: str,
    account_name: str,
) -> None:
    result = cli_runner.invoke(
        configure,
        [],
        input=f"{service_id}\n{service_secret}\n{account_name}"
        f"\n{database_name}\n{engine_name}\n",
    )
    assert result.exit_code == 0, result.stderr

    cli_runner.invoke(
        configure,
        [
            "--api-endpoint",
            api_endpoint,
        ],
    )
    assert result.exit_code == 0, result.stderr


@pytest.fixture
def cli_runner() -> CliRunner:
    runner = CliRunner(mix_stderr=False)
    # Use fake fs not to interfere with existing config
    with Patcher():
        # Intialize config dir
        makedirs(user_config_dir(), exist_ok=True)
        yield runner


@pytest.fixture
def mock_table_config() -> dict:
    return {
        "table_name": "lineitem",
        "columns": [
            {"name": "l_orderkey", "type": "BIGINT"},
            {"name": "l_partkey", "type": "BIGINT"},
            {"name": "l_suppkey", "type": "BIGINT"},
            {"name": "l_linenumber", "type": "INTEGER"},
            {"name": "l_quantity", "type": "BIGINT"},
            {"name": "l_extendedprice", "type": "BIGINT"},
            {"name": "l_discount", "type": "BIGINT"},
            {"name": "l_tax", "type": "BIGINT"},
            {"name": "l_returnflag", "type": "TEXT"},
            {"name": "l_linestatus", "type": "TEXT"},
            {"name": "l_shipdate", "type": "TEXT"},
            {"name": "l_commitdate", "type": "TEXT"},
            {"name": "l_receiptdate", "type": "TEXT"},
            {"name": "l_shipinstruct", "type": "TEXT"},
            {"name": "l_shipmode", "type": "TEXT"},
            {"name": "l_comment", "type": "TEXT"},
        ],
        "file_type": "PARQUET",
        "object_pattern": ["*.parquet"],
        "primary_index": ["l_orderkey", "l_linenumber"],
    }


@fixture
def resource_manager(
    service_id: str, service_secret: str, account_name: str, api_endpoint: str
) -> ResourceManager:
    return construct_resource_manager(
        client_id=service_id,
        client_secret=service_secret,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
