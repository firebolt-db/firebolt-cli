from typing import Callable

import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.table import create_external


def test_create_external_table_happy_path(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
):
    """check calling create external table
    with valid yaml and with minimum aws settings"""
    configure_cli()

    connect_function_mock = mocker.patch("firebolt_cli.table.connect")

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

    assert result.exit_code == 0
