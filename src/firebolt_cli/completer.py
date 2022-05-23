from typing import Iterator

from prompt_toolkit import HTML
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

keywords = [
    "ALTER DATABASE",
    "ALTER ENGINE",
    "ALTER TABLE",
    "AND",
    "AS",
    "AS SELECT",
    "ASC",
    "ATTACH ENGINE",
    "AUTO_STOP",
    "CASCADE",
    "COMPRESSION",
    "COPY",
    "CREATE AGGREGATING INDEX",
    "CREATE AND GENERATE AGGREGATING INDEX",
    "CREATE DATABASE",
    "CREATE DIMENSION TABLE",
    "CREATE ENGINE",
    "CREATE EXTERNAL TABLE",
    "CREATE FACT TABLE",
    "CREATE JOIN INDEX",
    "CREATE VIEW",
    "CREDENTIALS",
    "CSV",
    "DEFAULT",
    "DESC",
    "DESCRIBE",
    "DROP",
    "DROP AGGREGATING INDEX",
    "DROP DATABASE",
    "DROP ENGINE",
    "DROP INDEX",
    "DROP JOIN INDEX",
    "DROP PARTITION",
    "DROP TABLE",
    "DROP VIEW",
    "EXPLAIN",
    "EXPLAIN USING JSON",
    "EXPLAIN USING TEXT",
    "FALSE",
    "FILE_NAME_PREFIX",
    "FROM",
    "GROUP BY",
    "GZIP",
    "HAVING",
    "IF EXISTS",
    "IF NOT EXISTS",
    "IN",
    "INCLUDE_QUERY_ID_IN_FILE_NAME",
    "INSERT INTO",
    "JSON",
    "LIMIT",
    "MAX_FILE_SIZE",
    "NONE",
    "NOT NULL",
    "NULL",
    "NULLS FIRST",
    "NULLS LAST",
    "OBJECT_PATTERN",
    "OFFSET",
    "ON",
    "OR",
    "OR REPLACE",
    "ORDER BY",
    "OVERWRITE_EXISTING_FILES",
    "PARQUET",
    "PARTITION",
    "PARTITION BY",
    "PRIMARY INDEX",
    "REFRESH ALL JOIN INDEXES ON TABLE",
    "REFRESH JOIN INDEX",
    "RENAME TO",
    "SCALE",
    "SELECT",
    "SELECT DISTINCT",
    "SET",
    "SHOW CACHE",
    "SHOW COLUMNS",
    "SHOW DATABASES",
    "SHOW ENGINES",
    "SHOW INDEXES",
    "SHOW TABLES",
    "SHOW VIEWS",
    "SINGLE_FILE",
    "SPEC",
    "START ENGINE",
    "STOP ENGINE",
    "TO",
    "TRUE",
    "TSV",
    "TYPE",
    "UNION",
    "UNIQUE",
    "URL",
    "VALUES",
    "WARMUP",
    "WHERE",
    "WITH",
]


class FireboltAutoCompleter(Completer):
    """
    Implements autocompletion for firebolt cli
    """

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        """
        Returns: a list of completions based on the document.text.
        Supports autocompletion by:
            - keywords

        """

        text = document.text
        delimiters = [" ", ",", "\n", ")", ";"]
        last_position = max([text.rfind(d) for d in delimiters])

        last_word = text[last_position + 1 :].upper()
        keyword_suggestions = [
            keyword for keyword in keywords if keyword.startswith(last_word)
        ]

        if len(last_word) == 0:
            return

        offset = len(last_word)
        for keyword in keyword_suggestions:
            yield Completion(
                keyword,
                start_position=-offset,
                display=HTML(
                    f"<b><style color='red'>{keyword[:offset]}</style></b>"
                    f"{keyword[offset:]}"
                ),
                display_meta="KEYWORD",
            )
