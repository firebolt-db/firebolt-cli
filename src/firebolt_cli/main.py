from click import group, version_option

from firebolt_cli import __version__


@group()
@version_option(__version__, "-V", "--version")
def main():
    pass
