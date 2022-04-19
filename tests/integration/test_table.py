import pytest
import yaml
from click.testing import CliRunner

from firebolt_cli.main import main


def drop_table(
    table_name: str, cli_runner: CliRunner, engine_name: str, database_name: str
):
    """
    Drop table by its name
    """
    sql = f"DROP table {table_name}"

    with open("drop_table.sql", "w") as f:
        f.write(sql)

    result = cli_runner.invoke(
        main,
        f"query --file drop_table.sql "
        f"--engine-name {engine_name} --database-name {database_name}".split(),
    )

    assert result.exit_code == 0


def check_table_exists(
    table_config: dict,
    cli_runner: CliRunner,
    engine_name: str,
    database_name: str,
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
        f"query --file num_columns.sql "
        f"--engine-name {engine_name} --database-name {database_name}".split(),
    )

    assert str(len(table_config["columns"]) + 2 * with_metadata) in result.stdout
    assert result.exit_code == 0


@pytest.mark.parametrize("with_metadata", [True, False])
def test_create_internal_table(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    engine_name: str,
    database_name: str,
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
        f"{'--add-file-metadata ' if with_metadata else ''}"
        f"--engine-name {engine_name} "
        f"--database-name {database_name}".split(),
    )
    assert result.exit_code == 0

    check_table_exists(
        mock_table_config,
        cli_runner,
        engine_name,
        database_name,
        with_metadata=with_metadata,
    )

    drop_table(
        mock_table_config["table_name"],
        cli_runner,
        engine_name,
        database_name,
    )


def test_create_external_table(
    configure_cli: None,
    mock_table_config: dict,
    cli_runner: CliRunner,
    engine_name: str,
    database_name: str,
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
        f"--engine-name {engine_name} "
        f"--database-name {database_name} "
        f"--s3-url {s3_url}".split(),
    )
    assert result.exit_code == 0

    mock_table_config["table_name"] = f"ex_{mock_table_config['table_name']}"

    check_table_exists(
        mock_table_config,
        cli_runner,
        engine_name,
        database_name,
        with_metadata=False,
    )

    drop_table(
        mock_table_config["table_name"],
        cli_runner,
        engine_name,
        database_name,
    )
