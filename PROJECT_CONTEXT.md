# WB Parser Project Context

Last verified: 2026-09-09 23:23 MSK.

## Purpose and stack

WB Parser is a Python Telegram bot for tracking Wildberries product positions, regional search positions, recommendation shelves, charts, alerts, and scheduled checks. It also contains an authorized cart-stock worker used by MpHub.

- Python 3.13 locally and 3.12 on production.
- `aiogram`, `aiohttp`, `curl_cffi`, `APScheduler`, `playwright`, SQLite, matplotlib, and openpyxl.
- Local repository: `/Users/octopus/Projects/wb-parser`.
- Production: host alias `wb-parser`, application path `~/wb-parser`.
- Repository: `imaxprom/wb-parser`, development branch `main`.

## Main components

- `bot.py`: Telegram handlers, persistent menu, scheduling, result formatting, and interactive WB authorization.
- `proxy_positions.py`: active position parser and shared authenticated WB request headers.
- `parser.py`: Geo scanner, recommendation shelf scanner, card/brand helpers, and legacy parser functions.
- `queue_worker.py`: fair serialized queue for ordinary position scans.
- `db.py`: global allow-list/token DB and per-user SQLite databases.
- `wb_health.py`: scoped WB access classification and cooldown state.
- `cart_stock_worker.py`: durable authorized cart-stock worker with SQLite outbox.
- `wb_session_runtime.py`: browser/session runtime support for cart-stock access.
- `scripts/wb_manual_auth_local.py`: manual local auth utility retained for diagnostics.
- `chrome_positions.py` and `wb_login.py`: old/fallback implementations, not the primary production path.

## User interface

- The main Telegram menu remains attached as a persistent reply keyboard.
- Section content is rendered as inline messages; navigation edits the current bot content message where possible.
- Ordinary search no longer performs a separate preflight request before the user-triggered parse.
- Geo and shelf scans are initiated manually from their respective menu sections.

## Wildberries request paths

### Main position search

- Working endpoint: `https://search.wb.ru/exactmatch/ru/common/v18/search`.
- Production has no `WB_PROXY_*`; WB requests run directly from the VPS through `curl_cffi.Session` and are sequential.
- Request state is derived from `data/wb_session.json` and includes current buyer auth/cookies and browser-like headers. Never print or document their values.
- Direct mode intentionally omits `X-Pow` for the `__direct__` token key; this is current tested behavior.

### Geo scanner

- Uses the same working `search.wb.ru` host and current buyer session.
- Keeps the 8 configured `dest` values and the previous five-page maximum.
- A production test confirmed that authenticated requests still respect `dest` and produce different positions by city.
- Geo lazily loads the persisted session, so it works immediately after bot restart.

### Recommendation shelves

- Current code is stale: it still uses `https://www.wildberries.ru/__internal/recom/recom/ru/common/v8/search`, which returns `498` from production.
- Verified replacement: `https://recom.wb.ru/recom/ru/common/v8/search`, which returned `200` and valid products for all seven tested competitors.
- This repair is intentionally still pending.

## WB authorization

- Owner starts auth in Telegram and sends a phone number and six-digit confirmation code.
- Production opens headed Chromium inside a private Xvfb display.
- Intermediate WB.ID state is saved separately from the active production session.
- Resume logic retains useful WB.ID state while dropping failed anti-bot challenge cookies, selects an existing account, and accepts the OAuth consent screen.
- A newly captured session is checked against a real search request before success is reported.
- Scheduled position parsing skips while an interactive authorization job is active.

## Runtime and deployment

- Services: `wb-parser.service` and `wb-cart-stock-worker.service`.
- Telegram API access on production uses a configured local proxy/tunnel; never print its credential-bearing configuration.
- Cart-stock worker runs under Xvfb with its browser proxy and systemd watchdog in the installed production unit.
- Standard code deployment: test locally, commit, push `main`, run `ssh wb-parser "~/wb-parser/deploy.sh"`, then inspect service logs.
- Production Git branch is `master`; operational deployment files on the server are intentionally untracked.

## Data and security

- Runtime data is under `data/` and is gitignored.
- User records live in `data/users/<user-folder>/user.db`; old aggregate tables remain in the global DB but are not the active per-user source of truth.
- Never store or expose `.env` values, cookies, session JSON content, tokens, passwords, API keys, proxy credentials, worker secrets, or database URLs.
- Existing unrelated working-tree changes belong to the user and must be preserved.

## Session-state command

This is not a Node project. `npm run save-session-state` currently fails because there is no root `package.json`; context is maintained manually in the Markdown files and Codex memory.
