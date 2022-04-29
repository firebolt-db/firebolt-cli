from click import group, version_option

from firebolt_cli import __version__
from firebolt_cli.configure import configure
from firebolt_cli.database import database
from firebolt_cli.engine import engine
from firebolt_cli.ingest import ingest
from firebolt_cli.query import query
from firebolt_cli.table import table
from firebolt_cli.utils import construct_shortcuts


@group(
    cls=construct_shortcuts(
        shortages={
            "config": "configure (config)",
            "configure": "configure (config)",
            "db": "database (db)",
            "database": "database (db)",
            "tb": "table (tb)",
            "table": "table (tb)",
        }
    )
)
@version_option(__version__, "-V", "--version")
def main() -> None:
    """
    Firebolt command line utility.
    """


main.add_command(ingest)
main.add_command(configure)
main.add_command(engine)
main.add_command(query)
main.add_command(database)
main.add_command(table)
