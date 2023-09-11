import pytest
import yaml
from click.testing import CliRunner

from firebolt_cli.main import main


def drop_table(table_name: str, cli_runner: CliRunner):
    """
    Drop table by its name
    """
    sql = f"DROP table {table_name}"

    result = cli_runner.invoke(main, ["query", "--sql", sql])

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

    result = cli_runner.invoke(main, ["query", "--sql", sql])

    assert str(len(table_config["columns"]) + 2 * with_metadata) in result.stdout
    assert result.exit_code == 0


def check_tables_equal_row_count(
    cli_runner: CliRunner,
    table_name_1: str,
    table_name_2: str,
):
    """
    Check that the provided tables have the same number of rows
    """

    sql = f"""
        SELECT (SELECT count(*) FROM {table_name_1}) ==
               (SELECT count(*) FROM {table_name_2}) as result
        """

    result = cli_runner.invoke(main, ["query", "--sql", sql, "--csv"])

    assert result.exit_code == 0
    assert "True" in result.stdout


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


@pytest.mark.skip(reason="Ingest functionality is broken right now on backend")))
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_ingest_mode(
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
    assert result.stderr == ""
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main,
        f"table create-fact " f"--file table_config.yaml --add-file-metadata".split(),
    )
    assert result.stderr == ""
    assert result.exit_code == 0

    result = cli_runner.invoke(
        main,
        f"ingest " f"--file table_config.yaml " f"--mode {mode}".split(),
    )
    assert result.stderr == ""
    assert result.exit_code == 0

    fact_table_name, external_table_name = (
        mock_table_config["table_name"],
        f"ex_{mock_table_config['table_name']}",
    )
    check_tables_equal_row_count(cli_runner, fact_table_name, external_table_name)
    for table_name in [fact_table_name, external_table_name]:
        drop_table(
            table_name,
            cli_runner,
        )
