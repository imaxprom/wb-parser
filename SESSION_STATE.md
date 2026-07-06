# WB Parser Session State

Last verified: 2026-07-06 17:19 MSK.

## Summary

- Application code fix verified at `7467d3d Add WB device id header`.
- Context documentation was saved after that fix; check Git HEAD for the latest context-only commit.
- Current production Git during verification: `7467d3d` on host alias `wb-parser`.
- Production service: `wb-parser.service` is active.
- Main issue fixed this session: direct Wildberries `__internal/search` requests returned `403 Angie` because WB now requires the browser `deviceid` header from `localStorage["wbx__sessionID"]`.
- Deployed fix: `proxy_positions._build_headers()` now sends `deviceid`, `x-spa-version: 14.2.3`, and `X-Userid`.

## Commands Run

- `npm run save-session-state` was attempted first, but failed because this Python project has no root `package.json`.
- Local syntax check passed: `./venv/bin/python -m py_compile proxy_positions.py`.
- Commit and push completed: `7467d3d Add WB device id header`.
- Production deploy completed after explicit `git pull --ff-only origin main` because the production branch is named `master` and has no upstream.

## Verification

Production verification after deploy:

- `bot._verify_current_wb_session_sync()` returned `(200, 300)`.
- `proxy_positions._build_headers("__direct__", with_bearer=True)` includes `deviceid`, `x-spa-version`, `X-Userid`, `Authorization`, and `Cookie`.
- Full `_fetch_keyword_sync()` for an organic sample returned `item_error=False`, `promo_pos=92`, `organic_pos=92`.
- Real user task after deploy processed 7 keywords in direct mode; logs show WB responses are no longer global `403`.

Local DB snapshot:

- `data/parser.db` exists, about 220 KB.
- Tables/counts: `alerts=3`, `allowed_users=4`, `articles=4`, `queries=24`, `results=1783`, `settings=3`, `wb_tokens=2`.

Production DB snapshot:

- `data/parser.db` exists, about 220 KB.
- Tables/counts: `alerts=3`, `allowed_users=4`, `articles=4`, `queries=24`, `results=1783`, `settings=3`, `wb_tokens=1`.

Environment summary without secrets:

- Local `.env` has `WB_PARSER_BOT_TOKEN`, `PARSE_MODE`, `WB_PROXY_1`, `WB_PROXY_2`; no `TELEGRAM_PROXY`.
- Production `.env` has `WB_PARSER_BOT_TOKEN`, `PARSE_MODE`, `TELEGRAM_PROXY`; no WB proxies configured, so WB parsing currently runs direct.

## Important Findings

- Evirma extension version checked: `2.41.1`.
- Evirma uses browser-context requests and adds `deviceid`, `X-Userid`, `X-Queryid`, optional personalization and AB-test data. The minimal field required to fix current production direct requests was `deviceid`.
- Local configured proxy entries returned proxy auth `407` during testing.
- A separately tested user-provided proxy reached WB through IPv6 egress and hit WB `498` challenge for direct API requests.

## Current Caveats

- `npm run save-session-state` is not available in this repository until a `package.json` script is added.
- Production worktree has untracked operational files (`deploy.sh`, `positions_rpc.py`, old `proxy_positions.py.bak-*`). They were not modified or removed.
- Some advertised products can still produce `organic_pos=None`; current business logic may mark that keyword as `ERROR` even when network/auth is healthy.
