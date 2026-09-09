# WB Parser Session State

Last verified: 2026-09-09 23:23 MSK.

## Current state

- Git is synchronized at `4f18b6e Load WB session before geo scan` on local `main`, `origin/main`, and production.
- Production Git branch is named `master`; `~/wb-parser/deploy.sh` fast-forwards it from `origin/main`.
- `wb-parser.service` and `wb-cart-stock-worker.service` are active with zero recorded restarts in their current runs.
- Local worktree has one pre-existing user change: `deploy/wb-cart-stock-worker.service`. Do not overwrite or discard it. Its Xvfb/browser-proxy/watchdog settings match the currently installed production unit.
- Production has untracked operational files and backups (`deploy.sh`, `positions_rpc.py`, `deploy-backups/`, `proxy_positions.py.bak-*`). Do not remove them without explicit approval.

## Latest completed work

- The main Telegram menu is a persistent reply keyboard; content screens use inline navigation without removing the main menu.
- Main WB position requests were simplified and use the working authenticated host `https://search.wb.ru/exactmatch/ru/common/v18/search`.
- Classified WB failures such as auth expiry, anti-bot, and rate limiting are no longer blindly retried as bursts.
- WB authorization now runs in headed Chromium under Xvfb, supports phone/code entry, resumes saved WB.ID state, selects the saved account, and accepts the WB.ID OAuth consent screen.
- A fresh production WB session was saved on 2026-09-04. At verification it contained the required Bearer, PoW, and wbaas state; no values are stored here.
- Geo scanner was restored in `65bf578` and `4f18b6e`: it now uses the working search host, current authenticated session, and loads that session itself after service restart.
- Real production Geo verification returned positions in all 8 configured regions: `МСК 10`, `СПБ 7`, `КРД 7`, `КЗН 8`, `ЕКБ 6`, `НСК 6`, `ХБР 7`, `ВЛД 6` for the tested article/query.

## Verification performed

- `npm run save-session-state` was attempted first and failed with `ENOENT` because this Python repository has no `package.json`.
- Local test suite: 24 tests passed.
- Production scheduled position scans after the Geo deploy continued returning real positions without global `401/498` failures.
- Production bot logs after the latest restart contain no application exceptions.
- Live endpoint diagnosis:
  - `www.wildberries.ru/__internal/search/...` returns `498` from production.
  - `search.wb.ru/exactmatch/...` returns `200` with products.
  - `www.wildberries.ru/__internal/recom/...` returns `498`.
  - `recom.wb.ru/recom/...` returns `200` with recommendation products.

## Data snapshot

All counts are metadata only; no user content or credentials are recorded.

- Local global DB: 4 allowed users, 2 legacy WB-token rows.
- Local allowed-user DB totals: 8 articles, 57 queries, 14 competitors, 89,790 results.
- Production global DB: 4 allowed users, 1 legacy WB-token row.
- Production allowed-user DB totals: 9 articles, 59 queries, 21 competitors, 359,216 results.
- Production has 2 auto-enabled articles and 12 auto keywords, all belonging to the owner. Other users currently contribute no scheduled WB load.
- Production scheduler interval is 20 minutes. The current parser uses four WB page requests per keyword, so the auto workload is about 48 WB requests per cycle.
- Cart-stock outbox is empty locally and on production.

## Environment summary

- Local: Python 3.13.12, `PARSE_MODE=proxy`, 2 WB proxy entries configured, no Telegram proxy, no MpHub cart-stock connection.
- Production: Python 3.12.3, `PARSE_MODE=proxy`, no WB proxies (direct WB access), Telegram proxy configured, MpHub cart-stock connection configured.
- Production WB search health is `healthy`.
- Cart-stock health file retains an old anti-bot state from 2026-09-08; the worker is active and idle, and reloads the WB session while waiting for work.

## Unfinished

- Recommendation shelf scanner is not fixed yet. `parser.RECOM_URL` still points to the blocked `www/__internal/recom` address. A read-only production probe proved that changing only the host to `recom.wb.ru` returns valid shelf positions.
- Geo still preserves its old request depth and presentation as requested. It can generate a large request batch when an article is absent, and it renders an HTTP failure as a normal dash.
- There are no dedicated shelf behavior tests. Geo now has a regression test for endpoint, auth, lazy session loading, and position extraction.
- `KnowledgeBase.tsx` does not exist because this repository has no React UI.

## Next session start

1. Read `SESSION_STATE.md`, `PROJECT_CONTEXT.md`, `TODO.md`, and `CLAUDE.md`.
2. Run `git status --short` and preserve the existing service-unit change.
3. Confirm local/origin/production HEAD and both production services.
4. If asked to continue repairs, start with the minimal shelf-host replacement plus a regression test, then deploy and verify one real shelf scan.
