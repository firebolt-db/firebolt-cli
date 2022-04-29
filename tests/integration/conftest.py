from logging import getLogger
from os import environ

import pytest
from click.testing import CliRunner
from pytest import fixture

from firebolt_cli.configure import configure

LOGGER = getLogger(__name__)

DATABASE_NAME_ENV = "DATABASE_NAME"
USER_NAME_ENV = "USER_NAME"
PASSWORD_ENV = "PASSWORD"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"
ENGINE_URL_ENV = "ENGINE_URL"
ENGINE_NAME_ENV = "ENGINE_NAME"
STOPPED_ENGINE_URL_ENV = "STOPPED_ENGINE_URL"
STOPPED_ENGINE_NAME_ENV = "STOPPED_ENGINE_NAME"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def default_region() -> str:
    return "eu-west-1"


@fixture(scope="session")
def engine_url() -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="session")
def stopped_engine_url() -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


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
def username() -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def s3_url() -> str:
    return "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"


@fixture(scope="session")
def configure_cli(
    api_endpoint: str,
    password: str,
    username: str,
    database_name: str,
    engine_name: str,
) -> None:
    result = CliRunner().invoke(
        configure,
        [],
        input=f"{username}\n{password}\n\n{database_name}\n{engine_name}\n",
    )
    assert result.exit_code == 0

    CliRunner().invoke(
        configure,
        [
            "--api-endpoint",
            api_endpoint,
        ],
    )
    assert result.exit_code == 0


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


@pytest.fixture
def mock_table_config() -> dict:
    return {
        "table_name": "lineitem",
        "columns": [
            {"name": "l_orderkey", "type": "LONG"},
            {"name": "l_partkey", "type": "LONG"},
            {"name": "l_suppkey", "type": "LONG"},
            {"name": "l_linenumber", "type": "INT"},
            {"name": "l_quantity", "type": "LONG"},
            {"name": "l_extendedprice", "type": "LONG"},
            {"name": "l_discount", "type": "LONG"},
            {"name": "l_tax", "type": "LONG"},
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
