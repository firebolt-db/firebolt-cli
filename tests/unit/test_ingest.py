from typing import Callable
from unittest.mock import ANY

import pytest
from click.testing import CliRunner
from firebolt_ingest.table_service import TableService
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from firebolt_cli.ingest import ingest


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_ingest_happy_path(
    configure_cli: Callable,
    mocker: MockerFixture,
    fs: FakeFilesystem,
    mock_table_config: str,
    mode: str,
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

    connect_function_mock = mocker.patch("firebolt_cli.ingest.create_connection")

    result = CliRunner().invoke(
        ingest,
        [
            "--external-table-name",
            "ex_table",
            "--fact-table-name",
            "table",
            "--engine-name",
            "engine_name",
            "--mode",
            mode,
        ],
    )
    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    if mode == "overwrite":
        ts_ingest_full.assert_called_once_with(
            external_table_name="ex_table",
            internal_table_name="table",
            firebolt_dont_wait_for_upload_to_s3=ANY,
        )
    else:
        ts_ingest_append.assert_called_once_with(
            external_table_name="ex_table",
            internal_table_name="table",
            firebolt_dont_wait_for_upload_to_s3=ANY,
        )

    ts_verify_ingestion.assert_called_once_with(
        external_table_name="ex_table", internal_table_name="table"
    )

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

    result = CliRunner(mix_stderr=False).invoke(
        ingest,
        [
            "--external-table-name",
            "ex_table",
            "--fact-table-name",
            "table",
            "--engine-name",
            "engine_name",
        ],
    )
    connect_function_mock.assert_called_once()
    ts.assert_called_once()
    ts_ingest_full.assert_called_once_with(
        external_table_name="ex_table",
        internal_table_name="table",
        firebolt_dont_wait_for_upload_to_s3=ANY,
    )

    ts_verify_ingestion.assert_called_once_with(
        external_table_name="ex_table", internal_table_name="table"
    )

    assert result.exit_code != 0
    assert "discrepancy" in result.stderr
