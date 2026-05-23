from datetime import UTC, datetime, timedelta

from news_ingestion.config import SourceRegistry
from news_ingestion.pipeline import IngestionPipeline
from news_ingestion.schemas import NewsItem
from news_ingestion.settings import Settings


def _item(title: str, published_at: datetime | None) -> NewsItem:
    return NewsItem(
        source_id="test",
        source_type="fast_agency",
        external_id=title,
        url=f"https://example.com/{title}",
        title=title,
        text=f"{title} text",
        published_at=published_at,
        fetched_at=datetime.now(UTC),
        confidence=0.7,
    )


def test_bootstrap_selects_news_from_last_day(tmp_path) -> None:
    pipeline = IngestionPipeline(
        SourceRegistry(sources=[]),
        Settings(database_path=tmp_path / "news.sqlite3"),
    )
    now = datetime.now(UTC)

    selected = pipeline._select_bootstrap_items(
        [
            _item("fresh-1", now - timedelta(hours=2)),
            _item("fresh-2", now - timedelta(hours=20)),
            _item("old", now - timedelta(days=3)),
        ]
    )

    assert [item.title for item in selected] == ["fresh-1", "fresh-2"]


def test_bootstrap_falls_back_to_latest_three_when_no_recent_news(tmp_path) -> None:
    pipeline = IngestionPipeline(
        SourceRegistry(sources=[]),
        Settings(database_path=tmp_path / "news.sqlite3"),
    )
    old = datetime.now(UTC) - timedelta(days=3)

    selected = pipeline._select_bootstrap_items(
        [
            _item("old-1", old),
            _item("old-2", old),
            _item("old-3", old),
            _item("old-4", old),
        ]
    )

    assert [item.title for item in selected] == ["old-1", "old-2", "old-3"]
