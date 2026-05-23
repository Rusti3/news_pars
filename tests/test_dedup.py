from datetime import UTC, datetime, timedelta

from news_ingestion.dedup import rebuild_story_clusters, story_cluster_report
from news_ingestion.schemas import NewsItem
from news_ingestion.storage import connect, initialize_database, save_news_item


def _item(
    source_id: str,
    external_id: str,
    title: str,
    published_at: datetime,
) -> NewsItem:
    return NewsItem(
        source_id=source_id,
        source_type="fast_agency",
        external_id=external_id,
        url=f"https://example.com/{source_id}/{external_id}",
        title=title,
        text=f"{title} body from {source_id}",
        published_at=published_at,
        fetched_at=published_at + timedelta(minutes=1),
        confidence=0.7,
    )


def test_story_dedup_clusters_similar_news_between_sources_within_one_hour(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    initialize_database(db_path)
    published_at = datetime(2026, 5, 23, 4, 29, tzinfo=UTC)

    save_news_item(
        db_path,
        _item("vedomosti_news", "1", "Над Россией ночью сбили 348 БПЛА", published_at),
    )
    save_news_item(
        db_path,
        _item(
            "kommersant_news",
            "2",
            "Российские силы ПВО сбили 348 БПЛА за ночь",
            published_at + timedelta(minutes=2),
        ),
    )

    stats = rebuild_story_clusters(db_path)

    assert stats.total_news == 2
    assert stats.story_clusters == 1
    assert stats.clustered_items == 2
    assert stats.deduplicated_items == 1
    assert stats.unique_stories == 1

    report = story_cluster_report(db_path)
    assert len(report) == 1
    assert report[0].source_count == 2
    assert report[0].sources == ["kommersant_news", "vedomosti_news"]


def test_story_dedup_does_not_cluster_similar_news_from_same_source(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    initialize_database(db_path)
    published_at = datetime(2026, 5, 23, 16, 0, tzinfo=UTC)

    save_news_item(
        db_path,
        _item(
            "finam_company_news",
            "1",
            "Совет директоров ПИКа рекомендовал отказаться от дивидендов за 2025 год",
            published_at,
        ),
    )
    save_news_item(
        db_path,
        _item(
            "finam_company_news",
            "2",
            "Совет директоров ЮМГ рекомендовал не выплачивать дивиденды за 2025 год",
            published_at + timedelta(minutes=20),
        ),
    )

    stats = rebuild_story_clusters(db_path)

    assert stats.total_news == 2
    assert stats.story_clusters == 0
    assert stats.deduplicated_items == 0
    assert stats.unique_stories == 2

    with connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM story_cluster_items").fetchone()
    assert row["count"] == 0


def test_story_dedup_does_not_cluster_news_outside_one_hour_window(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    initialize_database(db_path)
    published_at = datetime(2026, 5, 23, 10, 0, tzinfo=UTC)
    title = "Минобороны сообщило об уничтожении 800 беспилотников за сутки"

    save_news_item(db_path, _item("vedomosti_news", "1", title, published_at))
    save_news_item(
        db_path,
        _item("kommersant_news", "2", title, published_at + timedelta(hours=2)),
    )

    stats = rebuild_story_clusters(db_path)

    assert stats.total_news == 2
    assert stats.story_clusters == 0
    assert stats.deduplicated_items == 0
    assert stats.unique_stories == 2


def test_story_dedup_does_not_cluster_generic_titles_between_sources(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    initialize_database(db_path)
    published_at = datetime(2026, 5, 23, 13, 0, tzinfo=UTC)

    save_news_item(
        db_path,
        _item("lukoil_official_releases", "1", "Пресс-релиз", published_at),
    )
    save_news_item(
        db_path,
        _item("rosneft_official_press", "2", "Пресс-релизы", published_at),
    )

    stats = rebuild_story_clusters(db_path)

    assert stats.total_news == 2
    assert stats.story_clusters == 0
    assert stats.deduplicated_items == 0
    assert stats.unique_stories == 2
