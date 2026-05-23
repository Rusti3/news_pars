from datetime import UTC

import httpx
import pytest

from news_ingestion.adapters.disclosure import _parse_message_detail, _parse_message_list
from news_ingestion.adapters.html import HTMLNewsAdapter
from news_ingestion.adapters.rftoday import RFTODAY_SECTIONS, RFTodayAdapter
from news_ingestion.adapters.rss import RSSAdapter
from news_ingestion.schemas import SourceConfig
from news_ingestion.settings import Settings


def _settings(tmp_path) -> Settings:
    return Settings(database_path=tmp_path / "news.sqlite3")


def _source(method: str, url: str = "https://example.com/news") -> SourceConfig:
    return SourceConfig(
        id=f"source_{method}",
        name="Source",
        type="fast_agency" if method == "rss" else "media_analysis",
        method=method,
        url=url,
        interval_seconds=30,
        trust_score=0.7,
    )


def test_rss_adapter_parses_feed(tmp_path) -> None:
    adapter = RSSAdapter(_source("rss", "https://example.com/feed.xml"), _settings(tmp_path))
    content = """
    <rss version="2.0">
      <channel>
        <item>
          <title>Company news</title>
          <link>https://example.com/news/1</link>
          <guid>item-1</guid>
          <description><![CDATA[<p>Market text</p>]]></description>
          <pubDate>Sat, 23 May 2026 09:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    items = adapter.parse_feed(content)

    assert len(items) == 1
    assert items[0].external_id == "item-1"
    assert items[0].title == "Company news"
    assert "Market text" in items[0].text
    assert items[0].published_at is not None
    assert items[0].published_at.tzinfo is not None


class StubHTMLAdapter(HTMLNewsAdapter):
    async def _fetch_html(self, url: str) -> str:
        if url.rstrip("/") == "https://example.com/news":
            return """
            <html><body>
              <a href="/news/item-1">Item 1</a>
              <a href="/news/item-2">Item 2</a>
              <a href="/news/old-item-3">Old Item 3</a>
            </body></html>
            """
        return """
        <html><body>
          <article>
            <h1>HTML title</h1>
            <time datetime="2026-05-23T09:00:00+00:00"></time>
            <p>Full article text from html fixture.</p>
          </article>
        </body></html>
        """


class PartiallyBrokenHTMLAdapter(StubHTMLAdapter):
    async def extract_article(self, url: str):
        if url.endswith("item-1"):
            raise httpx.HTTPStatusError(
                "not found",
                request=httpx.Request("GET", url),
                response=httpx.Response(404),
            )
        return await super().extract_article(url)


@pytest.mark.asyncio
async def test_html_adapter_discovers_and_extracts_article(tmp_path) -> None:
    config = SourceConfig(
        id="html_source",
        name="HTML Source",
        type="media_analysis",
        method="html",
        url="https://example.com/news",
        interval_seconds=30,
        trust_score=0.7,
        parser={
            "list_item_selector": "a[href]",
            "title_selector": "h1",
            "article_selector": "article",
            "link_allow_patterns": ["/news/"],
            "max_items": 1,
        },
    )
    adapter = StubHTMLAdapter(config, _settings(tmp_path))

    links = await adapter.discover_article_links()
    item = await adapter.extract_article(links[0])

    assert links == ["https://example.com/news/item-1"]
    assert item is not None
    assert item.external_id == "https://example.com/news/item-1"
    assert item.title == "HTML title"
    assert "Full article text" in item.text


@pytest.mark.asyncio
async def test_html_adapter_does_not_backfill_known_top_links(tmp_path) -> None:
    config = SourceConfig(
        id="html_source",
        name="HTML Source",
        type="media_analysis",
        method="html",
        url="https://example.com/news",
        interval_seconds=30,
        trust_score=0.7,
        parser={
            "list_item_selector": "a[href]",
            "link_allow_patterns": ["/news/"],
            "max_items": 2,
        },
    )
    adapter = StubHTMLAdapter(config, _settings(tmp_path))
    adapter.set_known_external_ids(
        {
            "https://example.com/news/item-1",
            "https://example.com/news/item-2",
        }
    )

    links = await adapter.discover_article_links()

    assert links == []


@pytest.mark.asyncio
async def test_html_adapter_skips_broken_article_links(tmp_path) -> None:
    config = SourceConfig(
        id="html_source",
        name="HTML Source",
        type="media_analysis",
        method="html",
        url="https://example.com/news",
        interval_seconds=30,
        trust_score=0.7,
        parser={
            "list_item_selector": "a[href]",
            "title_selector": "h1",
            "article_selector": "article",
            "link_allow_patterns": ["/news/"],
            "max_items": 2,
        },
    )
    adapter = PartiallyBrokenHTMLAdapter(config, _settings(tmp_path))

    items = [item async for item in adapter.iter_items()]

    assert len(items) == 1
    assert items[0].external_id == "https://example.com/news/item-2"


def test_disclosure_parser_reads_list_and_detail() -> None:
    list_html = """
    <div class="messages-subjects">
      <div class="item">
        <span class="date">23.05.2026 12:30</span>
        <div class="link"><a href="/message/1">Disclosure title</a></div>
      </div>
    </div>
    """
    detail_html = """
    <main class="main-col">
      <h1>Detail title</h1>
      <p>Detail disclosure text.</p>
    </main>
    """

    items = _parse_message_list(
        list_html,
        source_id="azipi_disclosure_messages",
        source_type="disclosure",
        trust_score=0.95,
        page_url="https://e-disclosure.azipi.ru/messages/list/day-23.05.2026/",
    )
    detail = _parse_message_detail(detail_html)

    assert len(items) == 1
    assert items[0].title == "Disclosure title"
    assert items[0].published_at is not None
    assert items[0].published_at.tzinfo is UTC
    assert detail.title == "Detail title"
    assert "Detail disclosure text." in detail.text


def test_rftoday_adapter_parses_section_list(tmp_path) -> None:
    adapter = RFTodayAdapter(
        _source("rftoday", "https://www.rftoday.ru/"),
        _settings(tmp_path),
    )
    html = """
    <div class="item">
      <a class="source" href="/archive/1" title="Agency">Agency</a>
      <span class="title">RFtoday title</span>
      <p>RFtoday summary</p>
      <b>12:00</b>
    </div>
    """

    entries = adapter._parse_list_page(RFTODAY_SECTIONS[0], html)

    assert len(entries) == 1
    assert entries[0].archive_url == "https://www.rftoday.ru/archive/1"
    assert entries[0].title == "RFtoday title"
    assert entries[0].summary == "RFtoday summary"
