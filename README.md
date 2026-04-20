# First Light AI

Discord chat-to-news automation for AI frontier updates.

This branch contains the local-first production path:

- export new Discord messages since the last successful run
- preserve raw chat locally for downstream DB ingestion
- scan message chunks with Gemma 4 26B
- keep rumors instead of dropping them, tagged as `category=rumor`
- deduplicate only exact same facts/events
- re-rank active articles into `top`, `main`, and `side`
- publish the updated static site assets
- write daily new-article JSON files for other local pipelines

Live preview:

- Main: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/a9a6b3f417be5221efd2969fe8da85ed/raw/index.html
- Archive: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/a9a6b3f417be5221efd2969fe8da85ed/raw/archive.html

## Runtime

Windows Task Scheduler owns the daily 08:00 KST trigger:

```text
Task name: First Light AI Daily
Schedule: daily at 08:00 KST
Action: wsl.exe -e bash -lc 'cd /home/pineapple/bunjum2/signal && ./run_cron.sh >> /tmp/signal_daily.log 2>&1'
```

The scheduled entrypoint is:

```bash
/home/pineapple/bunjum2/signal/run_cron.sh
```

The wrapper loads only the local `DISCORD_TOKEN` from `discord_export_config.env`, then runs:

```bash
/home/pineapple/miniconda3/bin/python3 run_hourly.py
```

`discord_export_config.env` is intentionally ignored and must never be committed.

## Public Files

- `docs/index.html`: front page
- `docs/archive.html`: full article archive
- `docs/articles.json`: accumulated public article state
- `exports/articles/YYYY-MM-DD.json`: per-day new article export for other local pipelines

## Discord Exporter

The repository includes wrapper code, not the exporter binary:

- `discord_export_linux.py` calls an installed `DiscordChatExporter.Cli`
- `discord_export_text_only.py` supports the Windows/PowerShell path
- `run_hourly.py` chooses the wrapper for the current environment

Install or provide the actual exporter locally. Do not commit binaries, tokens, raw exports, or local DB files.

## Security Boundary

Never commit:

- `discord_export_config.env`
- `*.env`
- `memory/`
- raw Discord exports
- SQLite DB files
- generated DB backups

The old planning document from the previous repository direction is preserved at `docs/legacy/chanjoon-original-plan.md`.
