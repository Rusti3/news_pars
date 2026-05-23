from datetime import UTC, datetime

from news_ingestion.schemas import NewsItem, SourceConfig
from news_ingestion.storage import (
    connect,
    count_news,
    initialize_database,
    known_external_ids,
    save_news_item,
    sync_sources,
)


def _source() -> SourceConfig:
    return SourceConfig(
        id="test_source",
        name="Test Source",
        type="fast_agency",
        method="rss",
        url="https://example.com/feed.xml",
        interval_seconds=30,
        trust_score=0.7,
    )


def _item(text: str) -> NewsItem:
    return NewsItem(
        source_id="test_source",
        source_type="fast_agency",
        external_id="https://example.com/news/1",
        url="https://example.com/news/1",
        title="Short title",
        text=text,
        summary="Summary",
        published_at=datetime(2026, 5, 23, 9, 0, tzinfo=UTC),
        fetched_at=datetime(2026, 5, 23, 9, 1, tzinfo=UTC),
        confidence=0.5,
        raw={"version": len(text)},
    )


def test_save_news_item_upserts_by_source_and_external_id(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    initialize_database(db_path)
    sync_sources(db_path, [_source()])

    first = save_news_item(db_path, _item("short text"))
    second = save_news_item(db_path, _item("longer text with more complete article body"))

    assert first.created is True
    assert second.created is False
    assert first.news_id == second.news_id
    assert count_news(db_path) == 1
    assert known_external_ids(db_path, "test_source") == {"https://example.com/news/1"}

    with connect(db_path) as conn:
        row = conn.execute("SELECT text, confidence, raw_json FROM news").fetchone()

    assert row["text"] == "longer text with more complete article body"
    assert row["confidence"] == 0.5
    assert '"version": 43' in row["raw_json"]
