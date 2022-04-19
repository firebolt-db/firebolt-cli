from typing import Callable

import yaml
from click.testing import CliRunner
from firebolt_ingest.service import TableService
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.table import create_external, create_fact


def test_create_external_table_happy_path(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
):
    """check calling create external table
    with valid yaml and with minimum aws settings"""
    configure_cli()
    ts = mocker.patch.object(TableService, "__init__", return_value=None)
    ts_create_ext = mocker.patch.object(
        TableService, "create_external_table", return_value=None
    )

    connect_function_mock = mocker.patch("firebolt_cli.table.create_connection")

    fs.create_file("table_config.yaml", contents=yaml.dump(mock_table_config))

    result = CliRunner().invoke(
        create_external,
        [
            "--engine-name",
            "engine_name",
            "--database-name",
            "database_name",
            "--s3-url",
            "s3://bucket-name/path",
            "--file",
            "table_config.yaml",
        ],
    )
    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    ts_create_ext.assert_called_once()

    assert result.exit_code == 0


def test_create_fact_table_happy_path(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
):
    """check calling create fact table
    with valid yaml"""
    configure_cli()
    ts = mocker.patch.object(TableService, "__init__", return_value=None)
    ts_create_int = mocker.patch.object(
        TableService, "create_internal_table", return_value=None
    )

    connect_function_mock = mocker.patch("firebolt_cli.table.create_connection")

    fs.create_file("table_config.yaml", contents=yaml.dump(mock_table_config))

    result = CliRunner().invoke(
        create_fact,
        [
            "--engine-name",
            "engine_name",
            "--database-name",
            "database_name",
            "--file",
            "table_config.yaml",
        ],
    )
    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    ts_create_int.assert_called_once()

    assert result.exit_code == 0
