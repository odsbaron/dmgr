from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse


@dataclass(frozen=True)
class DataRef:
    table: str
    filters: dict[str, str]

    @property
    def uri(self) -> str:
        query = urlencode(self.filters)
        return f"duckdb://{self.table}?{query}" if query else f"duckdb://{self.table}"

    @classmethod
    def parse(cls, uri: str) -> "DataRef":
        parsed = urlparse(uri)
        if parsed.scheme != "duckdb":
            raise ValueError(f"unsupported data ref scheme: {parsed.scheme}")
        table = parsed.netloc or parsed.path.lstrip("/")
        if not table:
            raise ValueError("duckdb data ref requires a table")
        return cls(table=table, filters=dict(parse_qsl(parsed.query, keep_blank_values=True)))

    def __str__(self) -> str:
        return self.uri

