from typing import Callable, Optional

import pytest
import yaml
from click.testing import CliRunner
from firebolt_ingest.table_service import TableService
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.ingest import ingest


@pytest.mark.parametrize(
    "mode, ex_table_name",
    [
        ("overwrite", None),
        ("overwrite", "custom_ex_table"),
        ("append", "custom_ex_table"),
    ],
)
def test_ingest_happy_path(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
    mode: str,
    ex_table_name: Optional[str],
):
    """check calling insert_full_overwrite"""
    configure_cli()

    ts = mocker.patch.object(TableService, "__init__", return_value=None)
    ts_ingest_full = mocker.patch.object(
        TableService, "insert_full_overwrite", return_value=None
    )
    ts_ingest_append = mocker.patch.object(
        TableService, "insert_incremental_append", return_value=None
    )

    ts_verify_ingestion = mocker.patch.object(
        TableService, "verify_ingestion", return_value=True
    )
    fs.create_file("table_config.yaml", contents=yaml.dump(mock_table_config))

    connect_function_mock = mocker.patch("firebolt_cli.ingest.create_connection")

    result = CliRunner().invoke(
        ingest,
        f"--file table_config.yaml --engine-name engine_name --mode {mode}".split(),
    )

    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    if mode == "overwrite":
        ts_ingest_full.assert_called_once_with(
            firebolt_dont_wait_for_upload_to_s3=False,
            advanced_mode=False,
            use_short_column_path_parquet=False,
        )
    else:
        ts_ingest_append.assert_called_once_with(
            firebolt_dont_wait_for_upload_to_s3=False,
            advanced_mode=False,
            use_short_column_path_parquet=False,
        )

    ts_verify_ingestion.assert_called_once_with()

    assert result.exit_code == 0


def test_ingest_verify_failed(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
):
    """check verification error is propagated correctly"""
    configure_cli()

    ts = mocker.patch.object(TableService, "__init__", return_value=None)
    ts_ingest_full = mocker.patch.object(
        TableService, "insert_full_overwrite", return_value=None
    )
    ts_verify_ingestion = mocker.patch.object(
        TableService, "verify_ingestion", return_value=False
    )

    connect_function_mock = mocker.patch("firebolt_cli.ingest.create_connection")
    fs.create_file("table_config.yaml", contents=yaml.dump(mock_table_config))

    result = CliRunner(mix_stderr=False).invoke(
        ingest, f"--file table_config.yaml --engine-name engine_name".split()
    )
    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    ts_ingest_full.assert_called_once_with(
        firebolt_dont_wait_for_upload_to_s3=False,
        advanced_mode=False,
        use_short_column_path_parquet=False,
    )

    ts_verify_ingestion.assert_called_once_with()

    assert result.exit_code != 0
    assert "discrepancy" in result.stderr
