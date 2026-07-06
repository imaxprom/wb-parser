# WB Parser TODO

Last verified: 2026-07-06 17:19 MSK.

## High Priority

- Monitor the next scheduled parses after `7467d3d` to confirm no recurring global `403`.
- Review business logic for advertised products where `promo_pos` is found but `organic_pos` is `None`; this can mark a keyword as `ERROR` even when WB auth/network is healthy.
- Decide whether to add a real `save-session-state` script for this Python project, since `npm run save-session-state` currently cannot work without `package.json`.

## Medium Priority

- Clean production Git hygiene in a maintenance window: untracked `deploy.sh`, `positions_rpc.py`, and old `proxy_positions.py.bak-*` exist on the server. Do not delete them without confirming ownership.
- Decide whether local `.env` proxy entries are still needed; local tests showed the currently configured proxy entries return `407`.
- If proxy mode is needed again, use fresh working proxies and verify exact `__internal/search` endpoint, not only the visible WB website.

## Low Priority

- Consider moving shared WB browser-like header construction into one helper to avoid drift between `parser.py`, `proxy_positions.py`, and bot verification code.
- Update `WB_API_GUIDE.md` with the newly required `deviceid` header if it is still used as active documentation.
- Remove stale local generated/debug artifacts only after confirming they are not used for current operations.
