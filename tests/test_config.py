from collections import Counter

from news_ingestion.config import SourceRegistry


def test_sources_yaml_shape() -> None:
    registry = SourceRegistry.load("configs/sources.yaml")

    assert len(registry.sources) == 35
    assert len(registry.enabled_sources()) == 28
    assert Counter(source.method for source in registry.sources) == {
        "html": 22,
        "rss": 11,
        "azipi_disclosure": 1,
        "rftoday": 1,
    }
    assert Counter(source.method for source in registry.enabled_sources()) == {
        "html": 15,
        "rss": 11,
        "azipi_disclosure": 1,
        "rftoday": 1,
    }


def test_disabled_sources_stay_in_registry() -> None:
    registry = SourceRegistry.load("configs/sources.yaml")

    assert [source.id for source in registry.sources if not source.enabled] == [
        "sber_official_ir",
        "gazprom_official_news",
        "alrosa_official_news",
        "aeroflot_official_ir_news",
        "severstal_official_news",
        "surgut_official_ir",
        "rbc_investments_candidate",
    ]
