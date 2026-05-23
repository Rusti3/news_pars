from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Iterable

from news_ingestion.config import SourceRegistry
from news_ingestion.pipeline import IngestionPipeline, SourceRunStats
from news_ingestion.scheduler import create_scheduler
from news_ingestion.settings import Settings, get_settings
from news_ingestion.storage import count_news, initialize_database, sync_sources


def main() -> None:
    _configure_output_encoding()

    parser = argparse.ArgumentParser(prog="news-pars")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db")
    subparsers.add_parser("list-sources")
    subparsers.add_parser("bootstrap")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("source_id", nargs="?")

    subparsers.add_parser("run")

    args = parser.parse_args()
    settings = get_settings()
    registry = SourceRegistry.load(settings.sources_config_path)

    if args.command == "init-db":
        initialize_database(settings.database_path)
        synced = sync_sources(settings.database_path, registry.sources)
        print(f"initialized {settings.database_path} ({synced} sources synced)")
        return

    if args.command == "list-sources":
        _print_sources(registry)
        return

    pipeline = IngestionPipeline(registry, settings)

    if args.command == "ingest":
        results = asyncio.run(_run_ingest(pipeline, args.source_id))
        _print_stats(results)
        print(f"total rows: {count_news(settings.database_path)}")
        return

    if args.command == "bootstrap":
        results = asyncio.run(pipeline.bootstrap())
        _print_stats(results)
        print(f"total rows: {count_news(settings.database_path)}")
        return

    if args.command == "run":
        asyncio.run(_run_scheduler(pipeline, settings))
        return


async def _run_ingest(
    pipeline: IngestionPipeline,
    source_id: str | None,
) -> list[SourceRunStats]:
    if source_id:
        return [await pipeline.run_source(source_id)]
    return await pipeline.run()


async def _run_scheduler(pipeline: IngestionPipeline, settings: Settings) -> None:
    pipeline.initialize()
    if settings.bootstrap_enabled:
        print(
            f"bootstrap started: last {settings.bootstrap_lookback_days} day(s), "
            f"fallback={settings.bootstrap_fallback_items}"
        )
        _print_stats(await pipeline.bootstrap())
        print(f"bootstrap finished: total rows={count_news(settings.database_path)}")
    scheduler = create_scheduler(pipeline)
    scheduler.start()
    print(
        f"scheduler started: {len(pipeline.enabled_sources())} sources, "
        f"database={settings.database_path}"
    )
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        scheduler.shutdown(wait=False)


def _print_sources(registry: SourceRegistry) -> None:
    for source in registry.sources:
        marker = "enabled" if source.enabled else "disabled"
        print(f"{source.id}\t{source.method}\t{marker}\t{source.interval_seconds}s\t{source.name}")


def _print_stats(results: Iterable[SourceRunStats]) -> None:
    for result in results:
        print(
            f"{result.source_id} [{result.mode}]: fetched={result.fetched} "
            f"selected={result.selected} saved={result.saved} "
            f"duplicates={result.duplicates} skipped={result.skipped} "
            f"errors={len(result.errors)} duration={result.duration_seconds}s"
        )
        for error in result.errors:
            print(f"  error: {error}")


def _configure_output_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


if __name__ == "__main__":
    main()
