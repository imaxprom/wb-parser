"""Configuration for WB Parser Bot."""

import os
from pathlib import Path

# Load .env if exists
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# Telegram
BOT_TOKEN = os.getenv("WB_PARSER_BOT_TOKEN", "")
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "")  # http://user:pass@host:port

# Parse mode: "proxy" or "chrome"
PARSE_MODE = os.getenv("PARSE_MODE", "proxy")

# Proxies (user:pass@host:port)
WB_PROXIES = []
for _k in ("WB_PROXY_1", "WB_PROXY_2", "WB_PROXY_3", "WB_PROXY_4"):
    _v = os.getenv(_k, "")
    if _v:
        WB_PROXIES.append(_v)

# WB API
WB_SEARCH_URL = "https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search"
WB_DEST = "-1257786"  # Moscow, ул. Никольская 7-9 (ПВЗ в центре)
WB_APP_TYPE = "64"
WB_ITEMS_PER_PAGE = 100

# Geo regions for geo-scanner
# ПВЗ в центре каждого города (из all-poo-fr-v9.json)
GEO_REGIONS = [
    {"name": "Москва",           "short": "МСК", "dest": "-1257786"},   # ул. Никольская 7-9
    {"name": "Санкт-Петербург",  "short": "СПБ", "dest": "-1198055"},   # Малая Садовая 4
    {"name": "Краснодар",        "short": "КРД", "dest": "12358062"},   # Коммунаров 109
    {"name": "Казань",           "short": "КЗН", "dest": "-2133462"},   # ул. Пушкина 16
    {"name": "Екатеринбург",     "short": "ЕКБ", "dest": "-5818883"},   # ул. Толмачёва 25
    {"name": "Новосибирск",      "short": "НСК", "dest": "-364764"},    # ул. Якушева 58
    {"name": "Хабаровск",        "short": "ХБР", "dest": "-1785058"},   # ул. Постышева 18
    {"name": "Владивосток",      "short": "ВЛД", "dest": "123587791"},  # Семёновская 30
]

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "parser.db")
CHARTS_DIR = os.path.join(DATA_DIR, "charts")

# Ensure dirs exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)
