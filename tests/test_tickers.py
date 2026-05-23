import json
from datetime import UTC, datetime

from news_ingestion.schemas import NewsItem, SourceConfig
from news_ingestion.storage import connect, initialize_database, save_news_item, sync_sources
from news_ingestion.tickers import TickerRegistry, tag_existing_news


def _registry() -> TickerRegistry:
    return TickerRegistry(
        {
            "LKOH": ["LKOH", "Лукойл", "ЛУКОЙЛ", "Лукойла"],
            "SBER": ["SBER", "Сбер", "Сбера", "Сбербанк", "Сбербанка"],
            "YDEX": ["YDEX", "Яндекс", "Яндекса", "Яндексу", "Яндексе", "Yandex"],
            "T": [
                "Т-Технологии",
                "Т-Технологий",
                "Т-Банк",
                "Т-Банка",
                "ТБанк",
                "ТБанка",
                "Тинькофф",
                "TCS",
            ],
            "X5": ["X5", "X5 Group", "ИКС 5", "Пятёрочка", "Перекрёсток"],
            "MOEX": ["MOEX", "Московская биржа", "Московской биржи", "Мосбиржа", "Мосбиржи"],
        }
    )


def _source(source_id: str = "media", tickers: list[str] | None = None) -> SourceConfig:
    return SourceConfig(
        id=source_id,
        name=source_id,
        type="fast_agency",
        method="rss",
        tickers=tickers or ["ALL"],
        url="https://example.com/feed.xml",
        interval_seconds=30,
        trust_score=0.7,
    )


def _item(
    *,
    source_id: str = "media",
    external_id: str = "1",
    title: str = "Title",
    text: str = "Text",
) -> NewsItem:
    return NewsItem(
        source_id=source_id,
        source_type="fast_agency",
        external_id=external_id,
        url=f"https://example.com/{external_id}",
        title=title,
        text=text,
        published_at=datetime(2026, 5, 23, 9, 0, tzinfo=UTC),
        fetched_at=datetime(2026, 5, 23, 9, 1, tzinfo=UTC),
        confidence=0.7,
    )


def test_ticker_registry_matches_configured_terms_with_word_boundaries() -> None:
    matches = _registry().tag_text(
        title="ЛУКОЙЛ, SBER, Yandex и Пятерочка обсуждают индекс Мосбиржи",
        text="Сбербанка и Лукойла тоже упомянули.",
    )

    assert [match.ticker for match in matches] == ["LKOH", "MOEX", "SBER", "X5", "YDEX"]


def test_ticker_registry_does_not_match_single_t_but_matches_tcs_and_t_bank() -> None:
    registry = _registry()

    single_letter = registry.tag_text(title="Акция T выросла", text="")
    explicit_terms = registry.tag_text(title="TCS и Т-Банк раскрыли данные", text="")

    assert "T" not in [match.ticker for match in single_letter]
    assert [match.ticker for match in explicit_terms] == ["T"]


def test_ticker_backfill_adds_source_fallback_for_ticker_source(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    source = _source("lukoil_official_releases", ["LKOH"])
    initialize_database(db_path)
    sync_sources(db_path, [source])
    save_news_item(
        db_path,
        _item(source_id=source.id, title="Пресс-релиз", text="Общий текст"),
    )

    stats = tag_existing_news(db_path, _registry(), [source])

    assert stats.tagged_news == 1
    assert stats.ticker_counts == {"LKOH": 1}
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT tickers_json FROM news WHERE source = ?",
            (source.id,),
        ).fetchone()
        ticker_row = conn.execute(
            "SELECT matched_by, matched_terms_json FROM news_tickers WHERE ticker = 'LKOH'"
        ).fetchone()

    assert json.loads(row["tickers_json"]) == ["LKOH"]
    assert ticker_row["matched_by"] == "source"
    assert json.loads(ticker_row["matched_terms_json"]) == [f"source:{source.id}"]


def test_all_sources_do_not_get_fallback_and_use_only_regex(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    source = _source("rbc_news_rss", ["ALL"])
    initialize_database(db_path)
    sync_sources(db_path, [source])
    save_news_item(db_path, _item(source_id=source.id, title="Пресс-релиз", text="Нет тикера"))

    stats = tag_existing_news(db_path, _registry(), [source])

    assert stats.tagged_news == 0
    assert stats.ticker_counts == {}


def test_save_news_item_writes_ticker_json_and_join_table(tmp_path) -> None:
    db_path = tmp_path / "news.sqlite3"
    source = _source()
    item = _item(title="Сбербанк и Яндекс объявили новость", text="")
    matches = _registry().tag_item(item, source)

    initialize_database(db_path)
    sync_sources(db_path, [source])
    result = save_news_item(db_path, item, ticker_matches=matches)

    assert result.created is True
    with connect(db_path) as conn:
        news_row = conn.execute(
            "SELECT tickers_json FROM news WHERE news_id = ?",
            (result.news_id,),
        ).fetchone()
        ticker_rows = conn.execute(
            "SELECT ticker, matched_by FROM news_tickers ORDER BY ticker"
        ).fetchall()

    assert json.loads(news_row["tickers_json"]) == ["SBER", "YDEX"]
    assert [(row["ticker"], row["matched_by"]) for row in ticker_rows] == [
        ("SBER", "regex"),
        ("YDEX", "regex"),
    ]
