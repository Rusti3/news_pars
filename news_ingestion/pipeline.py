from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Lock

from news_ingestion.adapters import build_adapter
from news_ingestion.cleaning import clean_text
from news_ingestion.config import SourceRegistry
from news_ingestion.schemas import NewsItem, SourceConfig
from news_ingestion.settings import Settings
from news_ingestion.storage import (
    SourceWatermark,
    get_source_watermark,
    initialize_database,
    known_external_ids,
    save_news_item,
    sync_sources,
    update_source_watermark,
)


@dataclass
class SourceRunStats:
    source_id: str
    fetched: int = 0
    saved: int = 0
    updated: int = 0
    skipped: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    errors: list[str] = field(default_factory=list)

    def finish(self) -> None:
        self.finished_at = datetime.now(UTC)
        if self.started_at is not None:
            self.duration_seconds = round(
                (self.finished_at - self.started_at).total_seconds(),
                3,
            )


@dataclass
class WatermarkAccumulator:
    external_id: str | None = None
    published_at: datetime | None = None

    def observe(self, item: NewsItem) -> None:
        if item.published_at is not None and (
            self.published_at is None or item.published_at > self.published_at
        ):
            self.published_at = item.published_at
            self.external_id = item.external_id or self.external_id
        elif self.external_id is None and item.external_id:
            self.external_id = item.external_id


class IngestionPipeline:
    def __init__(self, registry: SourceRegistry, settings: Settings):
        self.registry = registry
        self.settings = settings
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            initialize_database(self.settings.database_path)
            sync_sources(self.settings.database_path, self.registry.sources)
            self._initialized = True

    def enabled_sources(self) -> list[SourceConfig]:
        return self.registry.enabled_sources()

    def get_source(self, source_id: str) -> SourceConfig:
        return self.registry.get(source_id)

    async def run(self) -> list[SourceRunStats]:
        self.initialize()
        results: list[SourceRunStats] = []
        for source in self.enabled_sources():
            results.append(await self.run_source(source.id))
        return results

    async def run_source(self, source_id: str) -> SourceRunStats:
        self.initialize()
        source = self.get_source(source_id)
        stats = SourceRunStats(source_id=source.id, started_at=datetime.now(UTC))

        if not source.enabled:
            stats.errors.append("source is disabled")
            stats.finish()
            return stats

        try:
            adapter = build_adapter(source, self.settings)
            if hasattr(adapter, "set_known_external_ids"):
                adapter.set_known_external_ids(
                    known_external_ids(self.settings.database_path, source.id)
                )
            watermark = get_source_watermark(self.settings.database_path, source.id)
            if hasattr(adapter, "set_watermark"):
                adapter.set_watermark(
                    watermark.last_seen_external_id,
                    watermark.last_seen_published_at,
                )
            await self._stream_source_items(adapter, source, stats, watermark)
        except Exception as exc:
            stats.errors.append(str(exc))

        stats.finish()
        return stats

    async def _stream_source_items(
        self,
        adapter,
        source: SourceConfig,
        stats: SourceRunStats,
        watermark: SourceWatermark,
    ) -> None:
        watermark_accumulator = WatermarkAccumulator()

        async for item in adapter.iter_items():
            stats.fetched += 1
            item.discovered_at = datetime.now(UTC)
            if _is_older_than_watermark(item, watermark):
                stats.updated += 1
                continue

            normalized = normalize_item(item, source)
            if normalized is None:
                stats.skipped += 1
                continue

            result = save_news_item(self.settings.database_path, normalized)
            watermark_accumulator.observe(normalized)
            if result.created:
                stats.saved += 1
            else:
                stats.updated += 1

        update_source_watermark(
            self.settings.database_path,
            source.id,
            external_id=watermark_accumulator.external_id,
            published_at=watermark_accumulator.published_at,
            polled_at=datetime.now(UTC),
        )


def normalize_item(item: NewsItem, source: SourceConfig) -> NewsItem | None:
    item.text = clean_text(item.text)
    if not item.text:
        return None
    if item.external_id is not None:
        item.external_id = clean_text(item.external_id) or None
    if item.title:
        item.title = clean_text(item.title) or None
    if item.summary:
        item.summary = clean_text(item.summary) or None
    item.confidence = max(item.confidence, source.trust_score)
    return item


def _is_older_than_watermark(item: NewsItem, watermark: SourceWatermark) -> bool:
    if (
        item.external_id
        and watermark.last_seen_external_id
        and item.external_id == watermark.last_seen_external_id
    ):
        return True
    return (
        item.published_at is not None
        and watermark.last_seen_published_at is not None
        and item.published_at < watermark.last_seen_published_at
    )
