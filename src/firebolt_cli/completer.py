import threading
from typing import Dict, Iterator, List

from firebolt.common.exception import FireboltError
from firebolt.db import Cursor
from httpx import HTTPStatusError
from prompt_toolkit import HTML
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

from firebolt_cli.keywords import FUNCTIONS, KEYWORDS


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
    delimiters = [" ", ",", "\n", ")", ";", "(", "."]
    last_position = max([text.rfind(d) for d in delimiters])

    return text[last_position + 1 :]


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

        self.thread = threading.Thread(
            target=self.populate_table_and_column_names, args=(cursor,), kwargs={}
        )
        self.thread.start()

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

        except (FireboltError, HTTPStatusError):
            pass

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
        if len(last_word) == 0:
            return

        current_suggestions: List = self.suggestions.copy()

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
