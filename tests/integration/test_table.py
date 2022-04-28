import pytest
import yaml
from click.testing import CliRunner

from firebolt_cli.main import main


def drop_table(table_name: str, cli_runner: CliRunner):
    """
    Drop table by its name
    """
    sql = f"DROP table {table_name}"

    with open("drop_table.sql", "w") as f:
        f.write(sql)

    result = cli_runner.invoke(
        main,
        f"query --file drop_table.sql ".split(),
    )

    assert result.exit_code == 0


def check_table_exists(
    table_config: dict,
    cli_runner: CliRunner,
    with_metadata: bool,
):
    """
    Check that the table exists, and has the same amount of columns
    """

    sql = (
        f"SELECT count(*) AS column_count "
        f"FROM information_schema.columns "
        f"WHERE table_name = '{table_config['table_name']}'"
    )

    with open("num_columns.sql", "w") as f:
        f.write(sql)

    result = cli_runner.invoke(
        main,
        f"query --file num_columns.sql ".split(),
    )

    assert str(len(table_config["columns"]) + 2 * with_metadata) in result.stdout
    assert result.exit_code == 0


@pytest.mark.parametrize("with_metadata", [True, False])
def test_create_internal_table(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    with_metadata: bool,
):
    """
    create fact table and verify it exists
    """

    with open("table_config.yaml", "w") as f:
        f.write(yaml.dump(mock_table_config))

    result = cli_runner.invoke(
        main,
        f"table create-fact "
        f"--file table_config.yaml "
        f"{'--add-file-metadata ' if with_metadata else ''}".split(),
    )
    assert result.exit_code == 0

    check_table_exists(
        mock_table_config,
        cli_runner,
        with_metadata=with_metadata,
    )

    drop_table(
        mock_table_config["table_name"],
        cli_runner,
    )


def test_create_external_table(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    s3_url: str,
):
    """
    create external table and verify it exists
    """

    with open("table_config.yaml", "w") as f:
        f.write(yaml.dump(mock_table_config))

    result = cli_runner.invoke(
        main,
        f"table create-external "
        f"--file table_config.yaml "
        f"--s3-url {s3_url}".split(),
    )
    assert result.exit_code == 0

    mock_table_config["table_name"] = f"ex_{mock_table_config['table_name']}"

    check_table_exists(
        mock_table_config,
        cli_runner,
        with_metadata=False,
    )

    drop_table(
        mock_table_config["table_name"],
        cli_runner,
    )


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_ingest_full_overwrite(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    s3_url: str,
    mode: str,
):
    """
    create external and fact tables, do ingestion with different modes
    """

    with open("table_config.yaml", "w") as f:
        f.write(yaml.dump(mock_table_config))

    result = cli_runner.invoke(
        main,
        f"table create-external "
        f"--file table_config.yaml "
        f"--s3-url {s3_url}".split(),
    )
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main,
        f"table create-fact " f"--file table_config.yaml --add-file-metadata".split(),
    )
    assert result.exit_code == 0

    fact_table_name = mock_table_config["table_name"]
    external_table_name = f"ex_{fact_table_name}"

    result = cli_runner.invoke(
        main,
        f"ingest "
        f"--fact-table-name {fact_table_name} "
        f"--external-table-name {external_table_name} "
        f"--mode {mode}".split(),
    )
    assert result.exit_code == 0, result.stderr

    for table_name in [fact_table_name, external_table_name]:
        drop_table(
            table_name,
            cli_runner,
        )


def test_ingest_append(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    s3_url: str,
):
    """
    create external and fact tables, do ingest data partially
    """

    mock_table_config_sub = mock_table_config.copy()
    mock_table_config_sub["object_pattern"] = ["*2.parquet"]
    mock_table_config_sub["table_name"] = "lineitem_sub"

    for mock_table in [mock_table_config_sub, mock_table_config]:
        with open("table_config.yaml", "w") as f:
            f.write(yaml.dump(mock_table))

        result = cli_runner.invoke(
            main,
            f"table create-external "
            f"--file table_config.yaml "
            f"--s3-url {s3_url}".split(),
        )
        assert result.exit_code == 0

    result = cli_runner.invoke(
        main,
        f"table create-fact " f"--file table_config.yaml --add-file-metadata".split(),
    )

    assert result.exit_code == 0

    fact_table_name = mock_table_config["table_name"]
    external_table_name = f"ex_{mock_table_config['table_name']}"
    external_table_name_sub = f"ex_{mock_table_config_sub['table_name']}"

    result = cli_runner.invoke(
        main,
        f"ingest "
        f"--fact-table-name {fact_table_name} "
        f"--external-table-name {external_table_name_sub} "
        f"--mode append".split(),
    )
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main,
        f"ingest "
        f"--fact-table-name {fact_table_name} "
        f"--external-table-name {external_table_name} "
        f"--mode append".split(),
    )
    assert result.exit_code == 0

    for table_name in [fact_table_name, external_table_name, external_table_name_sub]:
        drop_table(
            table_name,
            cli_runner,
        )
