from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from news_ingestion.schemas import NewsItem, SourceConfig


@dataclass(frozen=True)
class SaveResult:
    news_id: str
    created: bool


@dataclass(frozen=True)
class SourceWatermark:
    last_seen_external_id: str | None = None
    last_seen_published_at: datetime | None = None
    last_polled_at: datetime | None = None


def initialize_database(database_path: str | Path) -> None:
    path = Path(database_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    with connect(path) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                method TEXT NOT NULL,
                url TEXT,
                interval_seconds INTEGER NOT NULL,
                trust_score REAL NOT NULL,
                enabled INTEGER NOT NULL,
                raw_config TEXT NOT NULL,
                last_seen_external_id TEXT,
                last_seen_published_at TEXT,
                last_polled_at TEXT
            );

            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES sources(id),
                external_id TEXT,
                url TEXT,
                source_type TEXT NOT NULL,
                title TEXT,
                text TEXT NOT NULL,
                summary TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                saved_at TEXT NOT NULL,
                confidence REAL NOT NULL,
                raw_json TEXT,
                UNIQUE(source_id, external_id)
            );

            CREATE INDEX IF NOT EXISTS ix_news_source_saved_at
                ON news(source_id, saved_at DESC);
            CREATE INDEX IF NOT EXISTS ix_news_published_at
                ON news(published_at DESC);
            """
        )


def connect(database_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(database_path), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def sync_sources(database_path: str | Path, sources: list[SourceConfig]) -> int:
    initialize_database(database_path)
    with connect(database_path) as conn:
        for source in sources:
            payload = source.model_dump(mode="json")
            conn.execute(
                """
                INSERT INTO sources (
                    id, name, type, method, url, interval_seconds, trust_score,
                    enabled, raw_config
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    type = excluded.type,
                    method = excluded.method,
                    url = excluded.url,
                    interval_seconds = excluded.interval_seconds,
                    trust_score = excluded.trust_score,
                    enabled = excluded.enabled,
                    raw_config = excluded.raw_config
                """,
                (
                    source.id,
                    source.name,
                    source.type,
                    source.method,
                    str(source.url) if source.url else None,
                    source.interval_seconds,
                    source.trust_score,
                    1 if source.enabled else 0,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
    return len(sources)


def known_external_ids(database_path: str | Path, source_id: str) -> set[str]:
    initialize_database(database_path)
    with connect(database_path) as conn:
        rows = conn.execute(
            """
            SELECT external_id
            FROM news
            WHERE source_id = ? AND external_id IS NOT NULL
            """,
            (source_id,),
        )
        return {str(row["external_id"]) for row in rows if row["external_id"]}


def get_source_watermark(database_path: str | Path, source_id: str) -> SourceWatermark:
    initialize_database(database_path)
    with connect(database_path) as conn:
        row = conn.execute(
            """
            SELECT last_seen_external_id, last_seen_published_at, last_polled_at
            FROM sources
            WHERE id = ?
            """,
            (source_id,),
        ).fetchone()
    if row is None:
        return SourceWatermark()
    return SourceWatermark(
        last_seen_external_id=row["last_seen_external_id"],
        last_seen_published_at=_parse_datetime(row["last_seen_published_at"]),
        last_polled_at=_parse_datetime(row["last_polled_at"]),
    )


def update_source_watermark(
    database_path: str | Path,
    source_id: str,
    *,
    external_id: str | None,
    published_at: datetime | None,
    polled_at: datetime,
) -> None:
    initialize_database(database_path)
    current = get_source_watermark(database_path, source_id)
    next_external_id = current.last_seen_external_id
    next_published_at = current.last_seen_published_at

    if published_at is not None and (
        next_published_at is None or published_at >= next_published_at
    ):
        next_published_at = published_at
        next_external_id = external_id or next_external_id
    elif external_id:
        next_external_id = external_id

    with connect(database_path) as conn:
        conn.execute(
            """
            UPDATE sources
            SET last_seen_external_id = ?,
                last_seen_published_at = ?,
                last_polled_at = ?
            WHERE id = ?
            """,
            (
                next_external_id,
                _format_datetime(next_published_at),
                _format_datetime(polled_at),
                source_id,
            ),
        )


def save_news_item(database_path: str | Path, item: NewsItem) -> SaveResult:
    initialize_database(database_path)
    now = datetime.now(UTC)
    item.saved_at = item.saved_at or now
    news_id = _news_id(item)

    with connect(database_path) as conn:
        existing = _find_existing(conn, item, news_id)
        if existing is None:
            conn.execute(
                """
                INSERT INTO news (
                    id, source_id, external_id, url, source_type, title, text, summary,
                    published_at, fetched_at, saved_at, confidence, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _insert_payload(news_id, item),
            )
            return SaveResult(news_id=news_id, created=True)

        conn.execute(
            """
            UPDATE news
            SET url = ?,
                title = ?,
                text = ?,
                summary = ?,
                published_at = ?,
                fetched_at = ?,
                saved_at = ?,
                confidence = ?,
                raw_json = ?
            WHERE id = ?
            """,
            _update_payload(existing, item),
        )
        return SaveResult(news_id=str(existing["id"]), created=False)


def count_news(database_path: str | Path) -> int:
    initialize_database(database_path)
    with connect(database_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM news").fetchone()
    return int(row["count"])


def _find_existing(
    conn: sqlite3.Connection,
    item: NewsItem,
    news_id: str,
) -> sqlite3.Row | None:
    if item.external_id:
        row = conn.execute(
            """
            SELECT *
            FROM news
            WHERE source_id = ? AND external_id = ?
            """,
            (item.source_id, item.external_id),
        ).fetchone()
        if row is not None:
            return row
    return conn.execute("SELECT * FROM news WHERE id = ?", (news_id,)).fetchone()


def _insert_payload(news_id: str, item: NewsItem) -> tuple[Any, ...]:
    return (
        news_id,
        item.source_id,
        item.external_id,
        item.url,
        item.source_type,
        item.title,
        item.text,
        item.summary,
        _format_datetime(item.published_at),
        _format_datetime(item.fetched_at),
        _format_datetime(item.saved_at),
        item.confidence,
        _json_dumps(item.raw),
    )


def _update_payload(existing: sqlite3.Row, item: NewsItem) -> tuple[Any, ...]:
    title = _best_text(existing["title"], item.title)
    text = _best_text(existing["text"], item.text)
    summary = _best_text(existing["summary"], item.summary)
    published_at = existing["published_at"] or _format_datetime(item.published_at)
    raw_json = _merge_raw(existing["raw_json"], item.raw)
    confidence = max(float(existing["confidence"] or 0.0), item.confidence)

    return (
        item.url or existing["url"],
        title,
        text,
        summary,
        published_at,
        _format_datetime(item.fetched_at),
        _format_datetime(item.saved_at),
        confidence,
        raw_json,
        existing["id"],
    )


def _best_text(current: str | None, incoming: str | None) -> str | None:
    if not incoming:
        return current
    if not current or len(incoming) > len(current):
        return incoming
    return current


def _merge_raw(current: str | None, incoming: dict[str, Any] | None) -> str | None:
    if not incoming:
        return current
    if not current:
        return _json_dumps(incoming)
    try:
        current_payload = json.loads(current)
    except json.JSONDecodeError:
        current_payload = {"previous_raw_json": current}
    if isinstance(current_payload, dict):
        current_payload.update(incoming)
        return _json_dumps(current_payload)
    return _json_dumps(incoming)


def _news_id(item: NewsItem) -> str:
    return _stable_id(item.source_id, item.external_id, item.url, item.title, item.text)


def _stable_id(*parts: str | None) -> str:
    payload = "\x1f".join(part or "" for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
