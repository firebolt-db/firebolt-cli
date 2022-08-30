import re
from logging import getLogger
from typing import Dict, Iterator, List

from firebolt.common.exception import FireboltError
from firebolt.db import Cursor
from httpx import HTTPStatusError
from prompt_toolkit import HTML
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

from firebolt_cli.keywords import FUNCTIONS, KEYWORDS, SET_PARAMETERS

logger = getLogger(__name__)


def prepare_display_html(text: str, offset: int) -> HTML:
    """
    prepares a colored html from the text
    """
    return HTML(
        f"<b><style color='red'>{text[:offset]}</style></b>"
        f"{text[offset:].replace('<', '&#60;').replace('>', '&#62;')}"
    )


def extract_last_word(text: str) -> str:
    """
    return the last word from the string
    """
    words = re.split("\\ |,|\n|\\)|;|\\(|\\.", text)
    return words[-1] if words else ""


def extract_last_complete_word(text: str) -> str:
    """
    extract last complete word (assuming that the last word could incomplete,
    return the word before it)
    """
    words = re.split("\\ |\n|;", text)
    return extract_last_word(words[-2]) if len(words) > 1 else ""


class FireboltAutoCompleter(Completer):
    """
    Implements autocompletion for firebolt cli
    """

    def __init__(self, cursor: Cursor):
        """
        Args:
            cursor: Cursor for executing queries and getting table and column names
        """
        self.table_columns_mapping: Dict[str, List] = {}
        self.suggestions: List = []
        self.suggestions.extend((keyword, "KEYWORD") for keyword in KEYWORDS)
        self.suggestions.extend((function, "FUNCTION") for function in FUNCTIONS)
        self.set_statements: List = [
            (function, "SET PARAMETER") for function in SET_PARAMETERS
        ]

        self.populate_table_and_column_names(cursor)

    def populate_table_and_column_names(self, cursor: Cursor) -> None:
        """
        populate suggestion with table and column names
        """
        try:
            cursor.execute(
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns"
            )

            data = cursor.fetchall()
            for tb_name, col_name, dtype in data:
                tb_name = str(tb_name)
                if tb_name not in self.table_columns_mapping:
                    self.table_columns_mapping[tb_name] = []

                self.table_columns_mapping[tb_name].append((col_name, dtype))

            self.suggestions.extend(
                (table_name, "TABLE")
                for table_name in self.table_columns_mapping.keys()
            )

        except (FireboltError, HTTPStatusError) as e:
            logger.info(
                f"Extraction of the list of table "
                f"and columns names failed with: {str(e)}"
            )

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        """
        Returns: a list of completions based on the document.text.
        Supports autocompletion by:
            - keywords
            - function names
            - table and columns names
        """
        last_word = extract_last_word(document.text_before_cursor).upper()
        last_full_word = extract_last_complete_word(document.text_before_cursor).upper()

        current_suggestions: List = []
        if last_full_word == "SET":
            current_suggestions.extend(self.set_statements)
        elif len(last_word) != 0:
            current_suggestions.extend(self.suggestions)

            for tb_name, columns in self.table_columns_mapping.items():
                if tb_name in document.text:
                    current_suggestions.extend(
                        (column_name, f"COLUMN ({column_type}, {tb_name})")
                        for column_name, column_type in columns
                    )

        offset = len(last_word)
        for label, meta in current_suggestions:
            if not label.upper().startswith(last_word):
                continue

            yield Completion(
                label,
                start_position=-offset,
                display=prepare_display_html(label, offset),
                display_meta=meta,
            )
