# WB Parser Project Context

Last verified: 2026-07-06 17:19 MSK.

## Purpose

Telegram bot for tracking Wildberries product positions by article and keyword. It stores per-user articles, queries, alerts, parse results, and settings in SQLite.

## Stack

- Python on Mac development and VPS production.
- `aiogram`, `aiohttp`, `curl_cffi`, `APScheduler`, `matplotlib`, `openpyxl`, `playwright`.
- SQLite in `data/parser.db`.
- Virtual environment: `./venv/`.

## Main Files

- `bot.py`: Telegram bot, scheduling, session health checks, WB session refresh/keepalive orchestration.
- `proxy_positions.py`: current main WB position parser via `curl_cffi`.
- `parser.py`: older grouped parser code and shared WB API details.
- `config.py`: environment loading and runtime constants.
- `db.py`: SQLite schema and persistence.
- `scripts/wb_manual_auth_local.py`: local manual WB auth helper that can run through a proxy and save session state.
- `wb_login.py`: older WB login/session helper.
- `chrome_positions.py`: old browser/AppleScript based parser, retained as fallback/reference.

## Runtime Topology

- Local development path: `/Users/octopus/Projects/wb-parser`.
- GitHub repository: `imaxprom/wb-parser`.
- Production host alias: `ssh wb-parser`.
- Production app path: `~/wb-parser`.
- Production service: `wb-parser.service`.
- Telegram API on production uses `TELEGRAM_PROXY`; do not print proxy values.
- Wildberries parsing on production currently has no `WB_PROXY_*` configured and runs direct from the VPS.

## Deployment

Standard flow:

1. Edit locally.
2. Commit and push to `main`.
3. Deploy on production.
4. Verify service and logs.

Production branch note:

- Production reports branch `master` with no upstream but is fast-forwarded from `origin/main`.
- If plain `git pull` complains about missing tracking info, use `git pull --ff-only origin main`, then run deploy/restart.
- Documentation-only context commits do not require restarting `wb-parser.service`.

## WB Auth And Search

Current direct search endpoint:

- `https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search`

Current required request state:

- Bearer token from `data/wb_session.json` localStorage key `wbx__tokenData`.
- `x_wbaas_token` cookie from cached token/session.
- `_wbauid` cookie for `X-Queryid`.
- `deviceid` header from localStorage key `wbx__sessionID`.
- `X-Userid` parsed from the Bearer JWT.
- `x-spa-version: 14.2.3`.

Known behavior:

- Missing `deviceid` caused production `403 Angie` on 2026-07-06.
- Adding only `deviceid` was enough to restore direct production search to HTTP `200` with products.
- `X-Pow` is intentionally not sent in direct mode; previous testing showed direct mode fails with it.
- Search is sequential in the current direct/proxy path to avoid WB anti-bot and proxy instability.

## Data And Secrets

- Do not commit or print `.env`, `data/`, session JSON, tokens, proxy credentials, Telegram bot token, cookies, or database URLs.
- `data/` is gitignored and contains live session/cache/profile/database artifacts.
- Documentation and Codex memories must contain only redacted environment summaries.
