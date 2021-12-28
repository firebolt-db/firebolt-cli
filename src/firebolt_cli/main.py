from click import group, version_option

from firebolt_cli import __version__
from firebolt_cli.configure import configure


@group()
@version_option(__version__, "-V", "--version")
def main():
    pass


main.add_command(configure)
