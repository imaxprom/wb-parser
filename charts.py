"""Chart generation for position history."""

import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta, timezone
from scipy.interpolate import make_interp_spline
from scipy.ndimage import gaussian_filter1d
from config import CHARTS_DIR
import db

MSK = timezone(timedelta(hours=3))


def _utc_to_msk(dt_str: str) -> datetime:
    """Convert UTC datetime string to MSK."""
    dt = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK)


def generate_article_chart(telegram_id: int, article_id: int, sku: str, days: int = 7) -> str | None:
    """Generate combined chart for all queries of an article. Smooth spline curves."""
    queries = db.get_queries(telegram_id, article_id)
    if not queries:
        return None

    fig, ax = plt.subplots(figsize=(12, 5))
    has_data = False

    for q in queries:
        history = db.get_history(telegram_id, article_id, q["id"], days)
        dates = []
        positions = []
        for r in history:
            if r["position"] is not None:
                dates.append(_utc_to_msk(r["checked_at"]))
                positions.append(r["position"])

        if len(dates) >= 2:
            label = q["query"][:30] + ("..." if len(q["query"]) > 30 else "")

            # Remove duplicate timestamps (keep last value)
            x_num = mdates.date2num(dates)
            unique_mask = np.diff(x_num, prepend=-1) > 0
            x_num = x_num[unique_mask]
            positions_arr = np.array(positions)[unique_mask]

            if len(x_num) >= 4:
                # Smooth spline + Gaussian filter for extra smoothness
                k = min(3, len(x_num) - 1)
                spl = make_interp_spline(x_num, positions_arr, k=k)
                x_smooth = np.linspace(x_num[0], x_num[-1], 500)
                y_smooth = spl(x_smooth)
                y_smooth = np.clip(y_smooth, 1, 100)
                y_smooth = gaussian_filter1d(y_smooth, sigma=8)
                y_smooth = np.clip(y_smooth, 1, 100)
                ax.plot(mdates.num2date(x_smooth), y_smooth, linewidth=2.5, label=label)
            else:
                pos_clipped = np.clip(positions_arr, 1, 100)
                ax.plot(dates[:len(pos_clipped)], pos_clipped, linewidth=2.5, label=label)
            has_data = True

    if not has_data:
        plt.close(fig)
        return None

    ax.invert_yaxis()
    ax.set_ylim(100, 0)  # 0 (top) to 100 (bottom), inverted
    ax.set_ylabel("Позиция")
    ax.set_title(f"SKU {sku} | Все запросы | {days}д", fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M", tz=MSK))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))

    plt.tight_layout()

    path = os.path.join(CHARTS_DIR, f"chart_{telegram_id}_{article_id}.png")
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return path
