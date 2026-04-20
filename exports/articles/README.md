# Daily New Article Exports

`run_hourly.py` writes one JSON file per KST date:

```text
exports/articles/YYYY-MM-DD.json
```

Each file contains only the articles newly accepted during that run, plus minimal metadata:

```json
{
  "schema_version": 1,
  "journal": "First Light AI",
  "date": "2026-04-20",
  "generated_at": "2026-04-20T12:00:00+09:00",
  "count": 0,
  "articles": []
}
```

Other local pipelines should consume these files instead of scraping the public HTML.
