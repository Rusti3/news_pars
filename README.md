# News Pars

Минимальный сборщик новостей из источников `configs/sources.yaml`.

Он оставляет только адаптеры источников из `epitaph76/News` и пишет все найденные
новости в локальную SQLite БД. ML, LLM, графы, тикерная дедупликация, PostgreSQL,
Alembic, API-админка и торговый слой удалены.

## Источники

Поддерживаются методы:

- `rss` - RSS/Atom-ленты;
- `html` - HTML-страницы со списком новостей и переходом внутрь статьи;
- `azipi_disclosure` - раскрытия с `e-disclosure.azipi.ru`;
- `rftoday` - секции `www/oil/gas/metal/agro/finance/hitech.rftoday.ru`.

`telegram_api` не используется.

В конфиге 35 источников. По умолчанию запускаются только записи без
`enabled: false`.

## Быстрый старт

```powershell
python -m pip install -e ".[dev]"
copy .env.example .env
news-pars init-db
news-pars list-sources
news-pars bootstrap
news-pars ingest moex_news
news-pars run
```

По умолчанию БД пишется в `news.sqlite3`. Таблица `news` хранит минимальную
append-only запись:

```json
{
  "news_id": "source:external_id_or_hash",
  "source": "interfax",
  "published_at_msk": "2026-05-21 12:03:18",
  "received_at_msk": "2026-05-21 12:03:25",
  "title": "...",
  "text": "...",
  "url": "...",
  "raw_payload_hash": "..."
}
```

Повторный приход той же новости игнорируется по `news_id`; уже сохраненные строки
не обновляются.

При `news-pars run` перед постоянным polling запускается bootstrap: каждый enabled
источник расширенно сканируется, сохраняются новости за последние сутки, а если у
источника таких новостей нет - последние 3 доступные новости. Отдельно этот проход
можно запустить командой `news-pars bootstrap`.

HTML-источники работают в real-time режиме: каждый проход смотрит только верхние
`max_items` ссылок из списка источника. Если эти ссылки уже есть в базе, адаптер
не идет ниже по странице и не делает бэкфилл старых публикаций.
