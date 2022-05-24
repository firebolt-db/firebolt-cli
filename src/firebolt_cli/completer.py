import threading
from typing import Iterator, List

from firebolt.common.exception import FireboltError
from firebolt.db import Cursor
from prompt_toolkit import HTML
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

from firebolt_cli.keywords import KEYWORDS


class FireboltAutoCompleter(Completer):
    """
    Implements autocompletion for firebolt cli
    """

    def __init__(self, cursor: Cursor):
        """
        Args:
            cursor: Cursor for executing queries and getting table and column names
        """
        self.column_names: List = []
        self.table_names: List[str] = []

        self.thread = threading.Thread(
            target=self.get_table_and_column_names, args=(cursor,), kwargs={}
        )
        self.thread.start()

    def get_table_and_column_names(self, cursor: Cursor) -> None:
        """ """
        try:
            cursor.execute(
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns"
            )

            data = cursor.fetchall()
            self.table_names = list(set(str(d[0]) for d in data))
            self.column_names = data

        except FireboltError:
            pass

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        """
        Returns: a list of completions based on the document.text.
        Supports autocompletion by:
            - keywords
            - table and columns names

        """
        text = document.text_before_cursor
        delimiters = [" ", ",", "\n", ")", ";"]
        last_position = max([text.rfind(d) for d in delimiters])

        last_word = text[last_position + 1 :].upper()

        suggestions: List = []
        suggestions.extend((table_name, "TABLE") for table_name in self.table_names)
        suggestions.extend(
            (column_name, f"COLUMN ({column_type}, {tb_name})")
            for tb_name, column_name, column_type in self.column_names
        )
        suggestions.extend((keyword, "KEYWORD") for keyword in KEYWORDS)

        if len(last_word) == 0:
            return

        offset = len(last_word)
        for label, meta in suggestions:

            if not label.upper().startswith(last_word):
                continue

            yield Completion(
                label,
                start_position=-offset,
                display=HTML(
                    f"<b><style color='red'>{label[:offset]}</style></b>"
                    f"{label[offset:]}"
                ),
                display_meta=meta,
            )
