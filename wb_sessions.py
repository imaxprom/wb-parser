"""Three isolated WB logins. SQLite publishes immutable credential bundles.

Only this module publishes credentials. A reader obtains a complete version in
one database read; probes never replace the process's working credentials.
Legacy files are maintained for rollback, not used by managed consumers.
"""

from contextlib import contextmanager
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

import config

MAX_SESSIONS = 3
CHECK_INTERVAL = 900
SCHEMA_VERSION = 2
logger = logging.getLogger(__name__)


def root():
    return Path(config.DATA_DIR) / "wb_sessions"


def enabled():
    return (root() / "ready.json").exists()


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def normalize_phone(raw):
    value = str(raw or "").strip()
    if not re.fullmatch(r"[+\d\s()-]+", value):
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 10 and digits.startswith("9"):
        return "7" + digits
    if len(digits) == 11 and digits.startswith(("7", "8")):
        return "7" + digits[1:]
    return None


def phone_from_bundle(bundle):
    """Read only account fields, never delivery contacts or masked/encrypted values."""
    storage = bundle.get("session", {}).get("localStorage", {})
    phones = set()
    for key, raw in storage.items():
        if key != "wbx__tokenData" and not key.startswith("user-info-"):
            continue
        try:
            profile = json.loads(raw) if isinstance(raw, str) else raw
        except ValueError:
            continue
        if not isinstance(profile, dict):
            continue
        for field in ("phone", "formattedPhoneMobile"):
            phone = normalize_phone(profile.get(field))
            if phone:
                phones.add(phone)
    return next(iter(phones)) if len(phones) == 1 else None


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp." + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def connection():
    db = sqlite3.connect(root() / "registry.db", timeout=15)
    db.row_factory = sqlite3.Row
    try:
        with db:
            yield db
    finally:
        db.close()


@contextmanager
def lock(name, *, blocking=True):
    root().mkdir(mode=0o700, parents=True, exist_ok=True)
    if not all(c.isalnum() or c in "_-" for c in name):
        raise ValueError("Invalid lock name")
    fd = os.open(root() / (name + ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield


@contextmanager
def _legacy_refresh_lock():
    fd = os.open(Path(config.DATA_DIR) / "wb_session_refresh.lock", os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def initialize():
    """Idempotent migration; the original files stay available for rollback."""
    if read_json(root() / "ready.json").get("version", 0) >= SCHEMA_VERSION:
        return
    with lock("migration"), _legacy_refresh_lock():
        if read_json(root() / "ready.json").get("version", 0) >= SCHEMA_VERSION:
            return
        path = root() / "registry.db"
        with connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, revision TEXT, phone TEXT,
                    created_at REAL NOT NULL, checked_at REAL NOT NULL DEFAULT 0,
                    success_at REAL NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'empty',
                    status_code INTEGER, retry_at REAL NOT NULL DEFAULT 0,
                    failures INTEGER NOT NULL DEFAULT 0,
                    cart_state TEXT NOT NULL DEFAULT 'unknown', cart_checked_at REAL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL,
                    session_id TEXT, kind TEXT NOT NULL, detail TEXT NOT NULL,
                    notified INTEGER NOT NULL DEFAULT 0
                );
            """)
            os.chmod(path, 0o600)
            if "phone" not in {row[1] for row in db.execute("PRAGMA table_info(sessions)")}:
                db.execute("ALTER TABLE sessions ADD COLUMN phone TEXT")
            if not db.execute("SELECT 1 FROM settings WHERE key='migrated'").fetchone():
                old = read_json(Path(config.DATA_DIR) / "wb_session.json")
                if old.get("localStorage") and old.get("cookies"):
                    sid = uuid.uuid4().hex[:12]
                    revision = uuid.uuid4().hex
                    bundle = {"session": old,
                              "browser": read_json(Path(config.DATA_DIR) / "wb_playwright_state.json"),
                              "login": read_json(Path(config.DATA_DIR) / "wb_login_working_state.json")}
                    atomic_json(root() / sid / (revision + ".json"), bundle)
                    db.execute("INSERT INTO sessions(id,name,revision,created_at,state) VALUES(?,?,?,?,?)",
                               (sid, "Сессия 1", revision, time.time(), "unknown"))
                    db.execute("INSERT INTO settings VALUES('active',?)", (sid,))
                    db.execute("INSERT INTO settings VALUES('generation',?)", (str(old.get("saved_at", 0)),))
                db.execute("INSERT INTO settings VALUES('migrated','1')")
            for row in db.execute("SELECT id,revision FROM sessions WHERE phone IS NULL AND revision IS NOT NULL").fetchall():
                bundle = read_json(root() / row["id"] / (row["revision"] + ".json"))
                phone = phone_from_bundle(bundle)
                if phone:
                    db.execute("UPDATE sessions SET phone=? WHERE id=?", (phone, row["id"]))
        atomic_json(root() / "ready.json", {"version": SCHEMA_VERSION})


def list_sessions():
    if not enabled():
        return []
    initialize()
    with connection() as db:
        return [dict(r) for r in db.execute("""SELECT s.*, s.id=COALESCE(
            (SELECT value FROM settings WHERE key='active'),'') AS active
            FROM sessions s ORDER BY created_at""")]


def create():
    initialize()
    with connection() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute("SELECT name FROM sessions").fetchall()
        if len(rows) >= MAX_SESSIONS:
            raise ValueError("Можно добавить не больше трёх сессий.")
        names = {r[0] for r in rows}
        name = next(f"Сессия {i}" for i in range(1, MAX_SESSIONS + 1) if f"Сессия {i}" not in names)
        sid = uuid.uuid4().hex[:12]
        db.execute("INSERT INTO sessions(id,name,created_at) VALUES(?,?,?)", (sid, name, time.time()))
        return sid


def snapshot(session_id=None):
    if not enabled():
        return {"session": read_json(Path(config.DATA_DIR) / "wb_session.json"),
                "browser": read_json(Path(config.DATA_DIR) / "wb_playwright_state.json"),
                "login": read_json(Path(config.DATA_DIR) / "wb_login_working_state.json"),
                "id": None, "revision": None}
    initialize()
    with connection() as db:
        row = db.execute("""SELECT s.*, (SELECT value FROM settings WHERE key='active') AS active_id,
            (SELECT value FROM settings WHERE key='generation') AS generation
            FROM sessions s WHERE s.id=COALESCE(?,(SELECT value FROM settings WHERE key='active'))""",
                         (session_id,)).fetchone()
    if not row:
        return {"session": {}, "browser": {}, "login": {}, "id": None, "revision": None}
    bundle = read_json(root() / row["id"] / (str(row["revision"]) + ".json"))
    data = dict(bundle.get("session", {}))
    if row["id"] == row["active_id"]:
        data["saved_at"] = float(row["generation"] or 0)
    data["pool_id"], data["pool_revision"] = row["id"], row["revision"]
    return {**bundle, "session": data, "id": row["id"], "revision": row["revision"],
            "name": row["name"], "phone": row["phone"]}


def bind_phone(sid, raw):
    """Bind once, including unfinished login attempts. Updating cannot change the account."""
    phone = normalize_phone(raw)
    if not phone:
        raise ValueError("Некорректный номер телефона.")
    initialize()
    with lock("slot_" + sid, blocking=False), connection() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT phone FROM sessions WHERE id=?", (sid,)).fetchone()
        if not row:
            raise ValueError("Сессия уже удалена. Откройте список заново.")
        if row["phone"] and row["phone"] != phone:
            raise ValueError("Сессия уже привязана к другому номеру. Для другого номера добавьте новую сессию.")
        db.execute("UPDATE sessions SET phone=? WHERE id=?", (phone, sid))
    return phone


def generation():
    return snapshot()["session"].get("saved_at", 0)


def _event(db, sid, kind, detail):
    db.execute("INSERT INTO events(at,session_id,kind,detail) VALUES(?,?,?,?)",
               (time.time(), sid, kind, detail))


def publish(sid, bundle, *, expected_revision=None, cancelled=None):
    """Publish only an already verified candidate. Never resurrect a deleted slot."""
    initialize()
    revision = uuid.uuid4().hex
    # The identity check prevents accidentally filling a reserve with a copy of
    # an existing login; the same WB account with independent tokens is allowed.
    session = bundle["session"]
    if not session.get("cookies") or not session.get("localStorage"):
        raise ValueError("Неполная авторизация WB.")
    atomic_json(root() / sid / (revision + ".json"), bundle)
    try:
        with connection() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
            if not old or (expected_revision is not None and old["revision"] != expected_revision):
                raise ValueError("Сессия уже изменена или удалена. Откройте список заново.")
            if cancelled and cancelled():
                raise ValueError("Вход отменён.")
            account_phone = phone_from_bundle(bundle)
            if old["phone"] and account_phone and old["phone"] != account_phone:
                raise ValueError("WB вернул другой аккаунт. Привязанный номер и прежняя сессия сохранены.")
            device = session.get("localStorage", {}).get("wbx__sessionID")
            for other in db.execute("SELECT id,revision FROM sessions WHERE id<>? AND revision IS NOT NULL", (sid,)):
                other_session = read_json(root() / other["id"] / (other["revision"] + ".json")).get("session", {})
                if (device and device == other_session.get("localStorage", {}).get("wbx__sessionID")) or (
                    session.get("localStorage", {}).get("wbx__tokenData") == other_session.get("localStorage", {}).get("wbx__tokenData")
                    and session.get("localStorage", {}).get("wbx__tokenData")):
                    raise ValueError("Этот вход уже сохранён в другой сессии. Выполните отдельный вход.")
            db.execute("UPDATE sessions SET revision=?,phone=COALESCE(phone,?),state='healthy',checked_at=?,success_at=?,failures=0,retry_at=0,status_code=200 WHERE id=?",
                       (revision, account_phone, time.time(), time.time(), sid))
            active = db.execute("SELECT value FROM settings WHERE key='active'").fetchone()
            active_row = db.execute("SELECT state FROM sessions WHERE id=?", (active[0],)).fetchone() if active else None
            replace_unavailable = active_row and active_row[0] in ("login_required", "auth_expired", "antibot")
            if not active or active[0] == sid or replace_unavailable:
                if active and active[0] != sid:
                    _event(db, sid, "switched", "Рабочая сессия: " + old["name"])
                db.execute("INSERT OR REPLACE INTO settings VALUES('active',?)", (sid,))
                db.execute("INSERT OR REPLACE INTO settings VALUES('generation',?)", (str(time.time_ns() / 1e9),))
            if old["state"] in ("login_required", "auth_expired", "antibot"):
                _event(db, sid, "restored", "Сессия восстановлена")
    except BaseException:
        (root() / sid / (revision + ".json")).unlink(missing_ok=True)
        raise
    try:
        export_legacy()
    except OSError:
        logger.warning("Could not update legacy session export; managed session remains committed")
    return snapshot(sid)


def activate(sid, *, expected_generation=None, expected_revision=None):
    with connection() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        current = db.execute("SELECT value FROM settings WHERE key='generation'").fetchone()
        if expected_generation is not None and float(current[0] if current else 0) != expected_generation:
            return False
        if not row or not row["revision"] or row["state"] != "healthy":
            raise ValueError("Сначала успешно проверьте сессию.")
        if expected_revision is not None and row["revision"] != expected_revision:
            return False
        db.execute("INSERT OR REPLACE INTO settings VALUES('active',?)", (sid,))
        db.execute("INSERT OR REPLACE INTO settings VALUES('generation',?)", (str(time.time_ns() / 1e9),))
        _event(db, sid, "switched", "Рабочая сессия: " + row["name"])
    try:
        export_legacy()
    except OSError:
        logger.warning("Could not update legacy session export; managed activation remains committed")
    return True


def export_legacy():
    # Serialize exports; a later publication is always re-read inside the lock.
    with lock("export"):
        value = snapshot()
        if not value.get("id"):
            return
        for name, key in (("wb_session.json", "session"), ("wb_playwright_state.json", "browser"),
                          ("wb_login_working_state.json", "login")):
            atomic_json(Path(config.DATA_DIR) / name, value.get(key, {}))


def remove(sid):
    with lock("slot_" + sid, blocking=False), connection() as db:
        db.execute("BEGIN IMMEDIATE")
        active = db.execute("SELECT value FROM settings WHERE key='active'").fetchone()
        if active and active[0] == sid:
            raise ValueError("Сначала сделайте рабочей другую проверенную сессию.")
        db.execute("DELETE FROM sessions WHERE id=?", (sid,))
    # No request uses a reserve's files without its snapshot/slot lock.
    import shutil
    shutil.rmtree(root() / sid, ignore_errors=True)


def record_health(value, result, *, scope="search"):
    if not enabled() or not value.get("id"):
        return
    state = result.get("state", "unknown")
    # A local/global pause says nothing about this credential's validity.
    if state in ("paused", "rate_limited", "busy"):
        return
    with connection() as db:
        row = db.execute("SELECT * FROM sessions WHERE id=? AND revision=?",
                         (value["id"], value["revision"])).fetchone()
        if not row:
            return
        if scope == "cart":
            db.execute("UPDATE sessions SET cart_state=?,cart_checked_at=? WHERE id=?",
                       (state, time.time(), value["id"]))
            return
        failures = 0 if state == "healthy" else row["failures"] + 1
        delay = (config.WB_SEARCH_498_RETRY_SECONDS
                 if state == "antibot" and result.get("status") == 498
                 else min(900 * 2 ** min(failures - 1, 3), 7200))
        retry = 0 if state == "healthy" else time.time() + delay
        db.execute("UPDATE sessions SET state=?,checked_at=?,success_at=?,status_code=?,failures=?,retry_at=? WHERE id=?",
                   (state, time.time(), time.time() if state == "healthy" else row["success_at"],
                    result.get("status"), failures, retry, value["id"]))
        if state != row["state"] and state != "healthy":
            _event(db, value["id"], "unavailable", row["name"] + ": " + state)


def pending_events():
    if not enabled():
        return []
    with connection() as db:
        return [dict(r) for r in db.execute("SELECT * FROM events WHERE notified=0 ORDER BY id LIMIT 30")]


def acknowledge_events(ids):
    with connection() as db:
        db.executemany("UPDATE events SET notified=1 WHERE id=?", [(i,) for i in ids])


def probe(value, *, emergency=False):
    """One real search, using only this snapshot. Never mutate global auth."""
    import proxy_positions as positions
    import wb_search_pacing as pacing
    import wb_search_recovery as recovery
    session = value.get("session", {})
    if not session.get("localStorage", {}).get("wbx__tokenData"):
        result = {"state": "login_required", "status": None}
        record_health(value, result)
        return result
    params = {"ab_testing": "false", "appType": "1", "curr": "rub", "dest": str(config.WB_DEST),
              "hide_dflags": "131072", "hide_dtype": "10;14", "inheritFilters": "false",
              "query": "трусы женские", "resultset": "catalog", "sort": "popular",
              "spp": "31", "suppressSpellcheck": "false", "limit": "100", "page": "1", "lang": "ru"}
    try:
        with pacing.client_scope("recovery" if emergency else "maintenance"):
            with pacing.request_slot(recovery=emergency) as policy:
                headers = positions._build_headers("__direct__", session_data=session)
                data, error = positions._search_unpaced_sync(headers, params, pacing=policy)
        if error:
            if error.get("error_state") == "rate_limited" or error.get("retry_after"):
                recovery.recover(error, recovery.session_generation(), allow_refresh=False)
            result = {"state": error.get("error_state", "network_error"),
                      "status": error.get("status_code"), "retry_after": error.get("retry_after", 0)}
        else:
            products = data.get("products", data.get("data", {}).get("products", [])) if isinstance(data, dict) else []
            valid = isinstance(products, list) and bool(products) and all(isinstance(p, dict) and "id" in p for p in products)
            result = {"state": "healthy" if valid else "invalid_response", "status": 200,
                      "products": len(products) if isinstance(products, list) else 0}
    except pacing.SearchPaused as exc:
        return {"state": "paused", "retry_after": exc.error.get("retry_after", 0)}
    except Exception:
        result = {"state": "network_error", "status": None}
    record_health(value, result)
    return result


def failover(error, observed_generation, *, scope="search", validator=None):
    """Called under the search recovery-operation lock, never under its state lock."""
    if not enabled():
        return None
    current = snapshot()
    if current["session"].get("saved_at", 0) != observed_generation:
        return {"state": "switched"}
    record_health(current, {"state": error.get("error_state", "unknown"), "status": error.get("status_code")}, scope=scope)
    candidates = [s for s in list_sessions() if not s["active"] and s["revision"]
                  and s["state"] not in ("login_required", "auth_expired") and s["retry_at"] <= time.time()]
    if scope != "search":
        candidates = [s for s in candidates if s["state"] == "healthy"]
    candidates.sort(key=lambda s: s["success_at"], reverse=True)
    limit = 1 if error.get("error_state") == "antibot" else MAX_SESSIONS - 1
    for candidate in candidates[:limit]:
        try:
            with lock("slot_" + candidate["id"], blocking=False):
                value = snapshot(candidate["id"])
                checked = validator(value) if validator else probe(value, emergency=True)
                if validator:
                    record_health(value, checked, scope=scope)
                if checked["state"] == "healthy":
                    changed = activate(candidate["id"], expected_generation=observed_generation,
                                       expected_revision=value["revision"])
                    return {"state": "switched" if changed or generation() != observed_generation else "busy"}
                if checked["state"] in ("paused", "rate_limited") or checked.get("retry_after"):
                    return checked
        except BlockingIOError:
            continue
    return None


def check_and_activate(sid):
    import wb_search_recovery as recovery
    with lock("switch"), lock("slot_" + sid, blocking=False):
        before = generation()
        value = snapshot(sid)
        result = probe(value, emergency=True)
        if result["state"] != "healthy":
            return result
        if recovery.cooldown_error(generation()):
            pause = recovery._read(recovery._state_path())
            if pause.get("server_delay"):
                return {"state": "paused"}
        if not activate(sid, expected_generation=before, expected_revision=value["revision"]):
            return {"state": "busy"}
        return {"state": "switched"}


def maintain():
    """One low-priority pass; auth-paused jobs may use recovery priority."""
    import wb_search_pacing as pacing
    import wb_search_recovery as recovery
    if not enabled():
        return
    def allowed():
        pause = recovery.cooldown_error(generation())
        state = recovery._read(recovery._state_path())
        if pause and (state.get("server_delay") or state.get("last_result") == "recovering"):
            return False, False
        urgent = bool(pause)
        return not pacing.bot_priority_active() or urgent, urgent
    if not allowed()[0]:
        return
    try:
        with lock("maintenance", blocking=False):
            for row in list_sessions():
                if row["active"] or not row["revision"] or row["state"] == "login_required":
                    continue
                due = (row["retry_at"] if row["state"] != "healthy" and row["retry_at"]
                       else row["checked_at"] + CHECK_INTERVAL)
                if due > time.time():
                    continue
                permitted, urgent = allowed()
                if not permitted:
                    break
                try:
                    with lock("slot_" + row["id"], blocking=False):
                        result = probe(snapshot(row["id"]), emergency=urgent)
                    if result["state"] == "healthy" and urgent:
                        check_and_activate(row["id"])
                    elif result["state"] in ("auth_expired", "antibot"):
                        from scripts.wb_session_monitor import refresh_subprocess
                        outcome = refresh_subprocess(session_id=row["id"])
                        if outcome.get("state") == "login_required":
                            record_health(snapshot(row["id"]), {"state": "login_required"})
                except BlockingIOError:
                    continue
    except BlockingIOError:
        pass
