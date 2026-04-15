"""Database layer for WB Parser.

Two database types:
- Global DB (parser.db): allowed_users, wb_tokens
- User DB (data/users/{id}_{name}/user.db): articles, queries, results, settings, alerts
"""

import os
import sqlite3
from contextlib import contextmanager
from typing import Optional
from config import DB_PATH, DATA_DIR

USERS_DIR = os.path.join(DATA_DIR, "users")
os.makedirs(USERS_DIR, exist_ok=True)

# Cache: telegram_id -> folder path
_folder_cache: dict[int, str] = {}


# ============================================================
# Connection managers
# ============================================================

@contextmanager
def _global_db():
    """Context manager for global DB."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@contextmanager
def _user_db(telegram_id: int):
    """Context manager for per-user DB."""
    path = _get_user_db_path(telegram_id)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _get_user_db_path(telegram_id: int) -> str:
    if telegram_id not in _folder_cache:
        with _global_db() as conn:
            row = conn.execute("SELECT folder_name FROM allowed_users WHERE telegram_id = ?",
                               (telegram_id,)).fetchone()
        folder_name = row["folder_name"] if row and row["folder_name"] else str(telegram_id)
        _folder_cache[telegram_id] = os.path.join(USERS_DIR, folder_name)

    folder = _folder_cache[telegram_id]
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, "user.db")


# ============================================================
# GLOBAL DB init
# ============================================================

def init_db():
    with _global_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT DEFAULT '',
                is_owner INTEGER DEFAULT 0,
                folder_name TEXT DEFAULT '',
                added_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS wb_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL,
                label TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                added_at TEXT DEFAULT (datetime('now')),
                last_used_at TEXT,
                last_error TEXT
            );
        """)

    # Run migrations for all existing users
    for u in get_allowed_users():
        _init_user_db(u["telegram_id"])


def _init_user_db(telegram_id: int):
    with _user_db(telegram_id) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE NOT NULL,
                name TEXT DEFAULT '',
                auto_check INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                query TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(article_id, query)
            );

            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                query_id INTEGER NOT NULL,
                position INTEGER,
                page INTEGER,
                total_found INTEGER DEFAULT 0,
                checked_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                FOREIGN KEY (query_id) REFERENCES queries(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_results_composite
                ON results(article_id, query_id, checked_at DESC);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT UNIQUE NOT NULL,
                enabled INTEGER DEFAULT 1,
                threshold INTEGER DEFAULT 0,
                last_fired_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS competitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                competitor_sku TEXT NOT NULL,
                competitor_name TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(article_id, competitor_sku)
            );
        """)

        for k, v in {"interval_minutes": "15", "pages_depth": "3"}.items():
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

        for atype, enabled, threshold in [
            ("position_drop_below", 0, 50),
            ("disappeared", 0, 0),
            ("position_change", 0, 10),
        ]:
            conn.execute("INSERT OR IGNORE INTO alerts (alert_type, enabled, threshold) VALUES (?, ?, ?)",
                         (atype, enabled, threshold))

        # Migration: add last_fired_at if missing
        try:
            conn.execute("ALTER TABLE alerts ADD COLUMN last_fired_at TEXT")
        except sqlite3.OperationalError:
            pass

        # Migration: create competitors table if missing (for existing users)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS competitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                competitor_sku TEXT NOT NULL,
                competitor_name TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(article_id, competitor_sku)
            )
        """)


# ============================================================
# Allowed Users
# ============================================================

def is_user_allowed(telegram_id: int) -> bool:
    with _global_db() as conn:
        return conn.execute("SELECT id FROM allowed_users WHERE telegram_id = ?",
                            (telegram_id,)).fetchone() is not None


def has_any_users() -> bool:
    with _global_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM allowed_users").fetchone()
        return row["cnt"] > 0


def is_owner(telegram_id: int) -> bool:
    with _global_db() as conn:
        row = conn.execute("SELECT is_owner FROM allowed_users WHERE telegram_id = ?",
                           (telegram_id,)).fetchone()
        return bool(row and row["is_owner"])


def add_user(telegram_id: int, username: str = "", is_owner: bool = False) -> Optional[int]:
    folder = f"{telegram_id}_{username}" if username else str(telegram_id)
    # Step 1: Insert user and commit (close connection first)
    with _global_db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO allowed_users (telegram_id, username, is_owner, folder_name) VALUES (?, ?, ?, ?)",
                (telegram_id, username, 1 if is_owner else 0, folder))
            lastrowid = cur.lastrowid
        except sqlite3.IntegrityError:
            return None

    # Step 2: Cache folder path BEFORE init (so _get_user_db_path doesn't query DB)
    _folder_cache[telegram_id] = os.path.join(USERS_DIR, folder)

    # Step 3: Init user DB (separate connection, no nesting)
    try:
        _init_user_db(telegram_id)
    except Exception:
        # Rollback: remove user if DB init fails
        with _global_db() as conn:
            conn.execute("DELETE FROM allowed_users WHERE telegram_id = ?", (telegram_id,))
        _folder_cache.pop(telegram_id, None)
        return None

    return lastrowid


def remove_user(telegram_id: int) -> bool:
    with _global_db() as conn:
        row = conn.execute("SELECT is_owner FROM allowed_users WHERE telegram_id = ?",
                           (telegram_id,)).fetchone()
        if row and row["is_owner"]:
            return False
        cur = conn.execute("DELETE FROM allowed_users WHERE telegram_id = ?", (telegram_id,))
        return cur.rowcount > 0


def get_allowed_users() -> list[dict]:
    with _global_db() as conn:
        rows = conn.execute("SELECT * FROM allowed_users ORDER BY is_owner DESC, added_at").fetchall()
        return [dict(r) for r in rows]


def update_user_username(telegram_id: int, username: str) -> bool:
    with _global_db() as conn:
        cur = conn.execute("UPDATE allowed_users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
        return cur.rowcount > 0


# ============================================================
# WB Tokens (global)
# ============================================================

def add_wb_token(token: str, label: str = "") -> Optional[int]:
    with _global_db() as conn:
        try:
            cur = conn.execute("INSERT INTO wb_tokens (token, label) VALUES (?, ?)", (token, label))
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None


def get_wb_tokens() -> list[dict]:
    with _global_db() as conn:
        rows = conn.execute("SELECT * FROM wb_tokens ORDER BY is_active DESC, added_at DESC").fetchall()
        return [dict(r) for r in rows]


def set_wb_token_active(token_id: int, active: bool):
    with _global_db() as conn:
        conn.execute("UPDATE wb_tokens SET is_active = ? WHERE id = ?", (1 if active else 0, token_id))


def remove_wb_token(token_id: int) -> bool:
    with _global_db() as conn:
        cur = conn.execute("DELETE FROM wb_tokens WHERE id = ?", (token_id,))
        return cur.rowcount > 0


def mark_wb_token_used(token_id: int):
    with _global_db() as conn:
        conn.execute("UPDATE wb_tokens SET last_used_at = datetime('now') WHERE id = ?", (token_id,))


def mark_wb_token_error(token_id: int, error: str):
    with _global_db() as conn:
        conn.execute("UPDATE wb_tokens SET last_error = ?, is_active = 0 WHERE id = ?", (error, token_id))


def get_next_wb_token() -> Optional[dict]:
    with _global_db() as conn:
        row = conn.execute(
            "SELECT * FROM wb_tokens WHERE is_active = 1 ORDER BY last_used_at ASC NULLS FIRST LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


# ============================================================
# Articles (per-user)
# ============================================================

def add_article(telegram_id: int, sku: str, name: str = "") -> Optional[int]:
    with _user_db(telegram_id) as conn:
        try:
            cur = conn.execute("INSERT INTO articles (sku, name) VALUES (?, ?)", (sku, name))
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None


def remove_article(telegram_id: int, sku: str) -> bool:
    with _user_db(telegram_id) as conn:
        cur = conn.execute("DELETE FROM articles WHERE sku = ?", (sku,))
        return cur.rowcount > 0


def remove_article_by_id(telegram_id: int, article_id: int) -> bool:
    with _user_db(telegram_id) as conn:
        cur = conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
        return cur.rowcount > 0


def get_articles(telegram_id: int) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute("SELECT * FROM articles ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


def get_article_by_sku(telegram_id: int, sku: str) -> Optional[dict]:
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT * FROM articles WHERE sku = ?", (sku,)).fetchone()
        return dict(row) if row else None


def get_article_by_id(telegram_id: int, article_id: int) -> Optional[dict]:
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
        return dict(row) if row else None


def update_article_name(telegram_id: int, article_id: int, name: str) -> bool:
    with _user_db(telegram_id) as conn:
        cur = conn.execute("UPDATE articles SET name = ? WHERE id = ?", (name, article_id))
        return cur.rowcount > 0


def toggle_auto_check(telegram_id: int, article_id: int) -> bool:
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT auto_check FROM articles WHERE id = ?", (article_id,)).fetchone()
        if not row:
            return False
        new_val = 0 if row["auto_check"] else 1
        conn.execute("UPDATE articles SET auto_check = ? WHERE id = ?", (new_val, article_id))
        return bool(new_val)


def get_auto_articles(telegram_id: int) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute("SELECT * FROM articles WHERE auto_check = 1 ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


# ============================================================
# Queries (per-user)
# ============================================================

def add_query(telegram_id: int, article_id: int, query: str) -> Optional[int]:
    with _user_db(telegram_id) as conn:
        try:
            row = conn.execute(
                "SELECT COALESCE(MAX(sort_order), 0) + 1 as next_order FROM queries WHERE article_id = ?",
                (article_id,)).fetchone()
            cur = conn.execute(
                "INSERT INTO queries (article_id, query, sort_order) VALUES (?, ?, ?)",
                (article_id, query.strip(), row["next_order"]))
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None


def remove_query(telegram_id: int, query_id: int) -> bool:
    with _user_db(telegram_id) as conn:
        cur = conn.execute("DELETE FROM queries WHERE id = ?", (query_id,))
        return cur.rowcount > 0


def get_queries(telegram_id: int, article_id: int) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute(
            "SELECT * FROM queries WHERE article_id = ? ORDER BY sort_order, id",
            (article_id,)).fetchall()
        return [dict(r) for r in rows]


def swap_query_order(telegram_id: int, query_id: int, direction: str):
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT * FROM queries WHERE id = ?", (query_id,)).fetchone()
        if not row:
            return
        current_order = row["sort_order"]
        article_id = row["article_id"]
        if direction == "up":
            neighbor = conn.execute(
                "SELECT * FROM queries WHERE article_id = ? AND sort_order < ? ORDER BY sort_order DESC LIMIT 1",
                (article_id, current_order)).fetchone()
        else:
            neighbor = conn.execute(
                "SELECT * FROM queries WHERE article_id = ? AND sort_order > ? ORDER BY sort_order ASC LIMIT 1",
                (article_id, current_order)).fetchone()
        if neighbor:
            conn.execute("UPDATE queries SET sort_order = ? WHERE id = ?", (neighbor["sort_order"], query_id))
            conn.execute("UPDATE queries SET sort_order = ? WHERE id = ?", (current_order, neighbor["id"]))


# ============================================================
# Results (per-user)
# ============================================================

def save_result(telegram_id: int, article_id: int, query_id: int, position: Optional[int],
                page: Optional[int], total_found: int = 0):
    with _user_db(telegram_id) as conn:
        conn.execute(
            "INSERT INTO results (article_id, query_id, position, page, total_found) VALUES (?, ?, ?, ?, ?)",
            (article_id, query_id, position, page, total_found))


def get_last_result(telegram_id: int, article_id: int, query_id: int) -> Optional[dict]:
    with _user_db(telegram_id) as conn:
        row = conn.execute(
            "SELECT * FROM results WHERE article_id = ? AND query_id = ? ORDER BY checked_at DESC LIMIT 1",
            (article_id, query_id)).fetchone()
        return dict(row) if row else None


def get_previous_result(telegram_id: int, article_id: int, query_id: int) -> Optional[dict]:
    with _user_db(telegram_id) as conn:
        row = conn.execute(
            "SELECT * FROM results WHERE article_id = ? AND query_id = ? ORDER BY checked_at DESC LIMIT 1 OFFSET 1",
            (article_id, query_id)).fetchone()
        return dict(row) if row else None


def get_history(telegram_id: int, article_id: int, query_id: int, days: int = 7) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute(
            "SELECT * FROM results WHERE article_id = ? AND query_id = ? AND checked_at >= datetime('now', ?) ORDER BY checked_at",
            (article_id, query_id, f"-{days} days")).fetchall()
        return [dict(r) for r in rows]


# ============================================================
# Settings (per-user)
# ============================================================

def get_setting(telegram_id: int, key: str) -> str:
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else ""


def set_setting(telegram_id: int, key: str, value: str):
    with _user_db(telegram_id) as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))


# ============================================================
# Alerts (per-user)
# ============================================================

def get_alerts(telegram_id: int) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute("SELECT * FROM alerts").fetchall()
        return [dict(r) for r in rows]


def toggle_alert(telegram_id: int, alert_type: str, enabled: bool):
    with _user_db(telegram_id) as conn:
        conn.execute("UPDATE alerts SET enabled = ? WHERE alert_type = ?",
                     (1 if enabled else 0, alert_type))


def set_alert_threshold(telegram_id: int, alert_type: str, threshold: int):
    with _user_db(telegram_id) as conn:
        conn.execute("UPDATE alerts SET threshold = ? WHERE alert_type = ?", (threshold, alert_type))


def mark_alert_fired(telegram_id: int, alert_type: str, query: str):
    """Mark that an alert was fired to prevent spam."""
    with _user_db(telegram_id) as conn:
        conn.execute("UPDATE alerts SET last_fired_at = datetime('now') WHERE alert_type = ?", (alert_type,))


def get_alert_last_fired(telegram_id: int, alert_type: str) -> Optional[str]:
    with _user_db(telegram_id) as conn:
        row = conn.execute("SELECT last_fired_at FROM alerts WHERE alert_type = ?", (alert_type,)).fetchone()
        return row["last_fired_at"] if row and row["last_fired_at"] else None


# ============================================================
# Competitors (per-user)
# ============================================================

MAX_COMPETITORS_PER_ARTICLE = 10


def add_competitor(telegram_id: int, article_id: int, competitor_sku: str, name: str = "") -> Optional[int]:
    with _user_db(telegram_id) as conn:
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM competitors WHERE article_id = ?",
            (article_id,)).fetchone()["cnt"]
        if count >= MAX_COMPETITORS_PER_ARTICLE:
            return None
        try:
            cur = conn.execute(
                "INSERT INTO competitors (article_id, competitor_sku, competitor_name) VALUES (?, ?, ?)",
                (article_id, competitor_sku, name))
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None


def remove_competitor(telegram_id: int, competitor_id: int) -> bool:
    with _user_db(telegram_id) as conn:
        cur = conn.execute("DELETE FROM competitors WHERE id = ?", (competitor_id,))
        return cur.rowcount > 0


def get_competitors(telegram_id: int, article_id: int) -> list[dict]:
    with _user_db(telegram_id) as conn:
        rows = conn.execute(
            "SELECT * FROM competitors WHERE article_id = ? ORDER BY id",
            (article_id,)).fetchall()
        return [dict(r) for r in rows]


def update_competitor_name(telegram_id: int, competitor_id: int, name: str):
    with _user_db(telegram_id) as conn:
        conn.execute("UPDATE competitors SET competitor_name = ? WHERE id = ?", (name, competitor_id))


def count_competitors(telegram_id: int, article_id: int) -> int:
    with _user_db(telegram_id) as conn:
        return conn.execute(
            "SELECT COUNT(*) as cnt FROM competitors WHERE article_id = ?",
            (article_id,)).fetchone()["cnt"]
