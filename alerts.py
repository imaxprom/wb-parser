"""Alert system for position changes with cooldown."""

from datetime import datetime, timedelta, timezone
import db

COOLDOWN_MINUTES = 60  # Don't repeat same alert type within 1 hour


def check_alerts(telegram_id: int, article_id: int, sku: str, results: list[dict]) -> list[str]:
    """Check results against alert rules. Skips errors. Respects cooldown."""
    alert_configs = {a["alert_type"]: a for a in db.get_alerts(telegram_id)}
    messages = []

    queries = db.get_queries(telegram_id, article_id)
    query_map = {q["query"]: q for q in queries}

    now = datetime.now(timezone.utc)

    for r in results:
        if r.get("error"):
            continue

        query = r["query"]
        position = r["position"]
        query_obj = query_map.get(query)
        if not query_obj:
            continue

        prev = db.get_previous_result(telegram_id, article_id, query_obj["id"])

        # Alert 1: position dropped below threshold
        cfg = alert_configs.get("position_drop_below", {})
        if cfg.get("enabled") and position is not None:
            if _check_cooldown(cfg):
                threshold = cfg.get("threshold", 50)
                if position > threshold:
                    messages.append(
                        f"⚠️ SKU {sku} | \"{query}\"\n"
                        f"Позиция {position} — ниже порога {threshold}")
                    db.mark_alert_fired(telegram_id, "position_drop_below", query)

        # Alert 2: disappeared from search
        cfg = alert_configs.get("disappeared", {})
        if cfg.get("enabled") and position is None:
            if _check_cooldown(cfg):
                if prev and prev.get("position") is not None:
                    messages.append(
                        f"🔴 SKU {sku} | \"{query}\"\n"
                        f"Товар пропал из выдачи! (был на позиции {prev['position']})")
                    db.mark_alert_fired(telegram_id, "disappeared", query)

        # Alert 3: position changed significantly
        cfg = alert_configs.get("position_change", {})
        if cfg.get("enabled") and position is not None:
            if _check_cooldown(cfg):
                threshold = cfg.get("threshold", 10)
                if prev and prev.get("position") is not None:
                    diff = position - prev["position"]
                    if abs(diff) >= threshold:
                        direction = "упала" if diff > 0 else "выросла"
                        emoji = "📉" if diff > 0 else "📈"
                        messages.append(
                            f"{emoji} SKU {sku} | \"{query}\"\n"
                            f"Позиция {direction}: {prev['position']} → {position} ({'+' if diff > 0 else ''}{diff})")
                        db.mark_alert_fired(telegram_id, "position_change", query)

    return messages


def _check_cooldown(cfg: dict) -> bool:
    """Check if enough time passed since last alert of this type."""
    last_fired = cfg.get("last_fired_at")
    if not last_fired:
        return True
    try:
        last_dt = datetime.fromisoformat(last_fired).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - last_dt) > timedelta(minutes=COOLDOWN_MINUTES)
    except Exception:
        return True
