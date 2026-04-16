# Правила работы с проектом WB Parser

## Изоляция
- Работай ТОЛЬКО внутри `/Users/octopus/Projects/wb-parser/`
- ЗАПРЕЩЕНО читать, писать, удалять файлы за пределами этой папки
- ЗАПРЕЩЕНО обращаться к другим проектам, копировать код из них, ссылаться на них

## База данных
- Чтение и редактирование .db файлов внутри папки проекта — разрешено

## Внешние подключения
- ЗАПРЕЩЕНО загружать внешние пакеты или зависимости без согласования

## Стек проекта
- Python 3.12 (VPS) / 3.13 (Mac), aiogram 3, aiohttp, curl_cffi, APScheduler, matplotlib, openpyxl, playwright
- SQLite (WAL mode) — per-user БД
- Виртуальное окружение: `./venv/`

## Архитектура: Mac (разработка) → GitHub → VPS (продакшн)

### Где что работает
- **Mac** (`/Users/octopus/Projects/wb-parser/`) — ТОЛЬКО разработка, код правим тут
- **GitHub** (`imaxprom/wb-parser`, private) — хранилище версий
- **VPS wb-parser** (`192.168.55.102`, user `makson`) — ПРОДАКШН, бот работает тут

### Деплой — ОБЯЗАТЕЛЬНЫЙ порядок после ЛЮБОГО изменения кода
1. Правим код на Mac
2. Коммитим: `git add <файлы> && git commit -m "описание"`
3. Пушим: `git push`
4. Деплоим: `ssh wb-parser "~/wb-parser/deploy.sh"`
5. Проверяем логи: `ssh wb-parser "sudo journalctl -u wb-parser --no-pager -n 10"`

**НИКОГДА** не забывай деплоить после изменений. Код на Mac без деплоя — мёртвый код.

### Откат
```
git log                    # история
git revert HEAD            # откат последнего коммита
git push
ssh wb-parser "~/wb-parser/deploy.sh"
```

## Подключение к VPS
- `ssh wb-parser` — подключение (через ProxyJump proxmox-jump)
- sudo пароль: `makson2026`
- Сервисы: `wb-parser.service`, `ssh-tunnel-telegram.service`
- Логи бота: `sudo journalctl -u wb-parser --no-pager -n 50`
- Перезапуск: `sudo systemctl restart wb-parser`

## Парсинг WB — ключевые решения
- **proxy_positions.py** — основной парсер (curl_cffi + авторизация покупателя)
- **Режим T07**: 4 параллельных fetch + Session, ~2.5 сек на 6 ключевиков, 100% стабильность
- **Авторизация**: Bearer + PoW + cookies из `data/wb_session.json` (получены через `wb_login.py`)
- **Без авторизации данные нестабильны** — рекламные позиции мигают
- **Retry**: если WB вернул пустые данные (error=True), автоматический повтор через 0.5 сек
- **chrome_positions.py** — старый подход (AppleScript, Mac only), сохранён как запасной

## Telegram на VPS
- Telegram API заблокирован на VPS напрямую
- Трафик идёт через SSH-туннель: `wb-parser:1080` → германская VPS `89.125.73.111`
- Сервис: `ssh-tunnel-telegram.service` (автозапуск, автореконнект)
- Настройка в `.env`: `TELEGRAM_PROXY=socks5://127.0.0.1:1080`

## Германская VPS (SSH-туннель)
- IP: `89.125.73.111`, root, порт 22
- На ней Amnezia (Docker, UDP 1274) — НЕ ТРОГАТЬ
- SSH-туннель wb-parser использует только SSH — конфликтов с Amnezia нет
- Эту VPS могут использовать и другие проекты для SSH-туннелей

## .env на VPS (НЕ в Git)
```
WB_PARSER_BOT_TOKEN=...
PARSE_MODE=proxy
TELEGRAM_PROXY=socks5://127.0.0.1:1080
```
