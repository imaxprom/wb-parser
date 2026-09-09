# WB Parser TODO

Last verified: 2026-09-09 23:23 MSK.

## High priority

- Restore recommendation shelves with the minimal verified change from blocked `www.wildberries.ru/__internal/recom/...` to `recom.wb.ru/recom/...`.
- Add a shelf regression test that checks the endpoint, authenticated headers, product extraction, and distinction between a missing product and an HTTP error.
- After the shelf fix, deploy and verify one real owner article against its configured competitors.
- Confirm the Geo result once from the Telegram button. The underlying production function already returned valid positions for all 8 cities after deploy.

## Medium priority

- Preserve but review the current Geo behavior: up to 5 pages per query/region can create a large batch when a product is absent.
- Make Geo display `ERR` for a WB/network failure instead of the same dash used for “not found,” and stop a batch after a classified `429/498`.
- Review scheduled load: 2 owner articles × 6 keywords × 4 page requests every 20 minutes, approximately 3,456 WB requests per day. Other users currently have no auto-enabled articles.
- Decide whether the local `deploy/wb-cart-stock-worker.service` change should be committed. It matches the installed production unit but is a pre-existing user change and must not be silently included in unrelated commits.
- Review the stale cart-stock health state if new cart-stock jobs fail; the worker itself is active and the outbox is empty.

## Low priority

- Consolidate WB endpoint constants and authenticated header/session loading so `parser.py` cannot drift from `proxy_positions.py` again.
- Decide whether to create a project-native session-state command. The requested npm command cannot exist naturally without adding Node metadata to this Python repository.
- Clean production untracked operational backups only in an explicit maintenance task with ownership confirmed.
- Review or remove the two stale local WB proxy entries only with approval; production does not use them.
- Update `WB_API_GUIDE.md` if it is still treated as current operational documentation.
