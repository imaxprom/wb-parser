# Правила работы с проектом WB Parser

Last verified: 2026-09-09 23:23 MSK.

## Изоляция и безопасность

- Работай с кодом только в `/Users/octopus/Projects/wb-parser/`, кроме явно запрошенного пользователем обновления памяти Codex.
- Чтение и редактирование SQLite внутри проекта разрешено.
- Не устанавливай внешние пакеты без согласования.
- Не печатай, не коммить и не записывай в документацию значения `.env`, cookies, session JSON, токены, пароли, API-ключи, прокси-учётные данные, worker secrets и database URLs.
- Существующие незакоммиченные изменения принадлежат пользователю. Сейчас таким изменением является `deploy/wb-cart-stock-worker.service`; сохраняй его отдельно от несвязанных задач.

## Архитектура

- Mac `/Users/octopus/Projects/wb-parser`: разработка.
- GitHub `imaxprom/wb-parser`, ветка `main`: источник версий.
- VPS по alias `ssh wb-parser`, путь `~/wb-parser`: production.
- Production-ветка называется `master`, но deploy fast-forward’ит её из `origin/main`.
- Python 3.13 локально, Python 3.12 на VPS, aiogram 3, curl_cffi, aiohttp, APScheduler, Playwright и SQLite.

## Обязательный порядок после изменения кода

1. Изменить код локально через аккуратный patch.
2. Запустить релевантные тесты; полный набор: `./venv/bin/python -m unittest discover -s tests -p '*_test.py'`.
3. Коммитить только относящиеся к задаче файлы и выполнить `git push`.
4. Выполнить `ssh wb-parser "~/wb-parser/deploy.sh"`.
5. Проверить статусы и свежие логи `wb-parser.service`; для worker-задач также `wb-cart-stock-worker.service`.

Документные context-only изменения можно пушить без перезапуска production-сервиса.

## Текущий runtime

- Проверенный код: `4f18b6e Load WB session before geo scan`.
- `wb-parser.service` и `wb-cart-stock-worker.service` активны.
- Основной парсер: `proxy_positions.py`, production работает напрямую без WB-прокси.
- Рабочий search endpoint: `https://search.wb.ru/exactmatch/ru/common/v18/search`.
- Geo использует этот же endpoint, действующую сессию и 8 прежних регионов; production-проверка успешна.
- Shelf scanner пока использует заблокированный `www/__internal/recom` endpoint. Проверенный, но ещё не внедрённый адрес: `https://recom.wb.ru/recom/ru/common/v8/search`.
- Полная локальная тестовая проверка: 24 теста.

## Авторизация WB

- Авторизация запускается владельцем через Telegram: телефон, затем шестизначный код.
- Используется headed Chromium в Xvfb, а не headless login.
- Рабочее промежуточное WB.ID-состояние отделено от активной session.
- Resume-путь выбирает сохранённый аккаунт и принимает OAuth-согласие.
- Новая session проверяется реальным search-запросом; scheduler пропускает цикл, пока идёт интерактивная авторизация.

## UI и нагрузка

- Главное Telegram-меню — постоянная ReplyKeyboard; inline-кнопки управляют содержимым разделов.
- Обычный поиск идёт через последовательную общую очередь и не делает отдельный preflight.
- Автопроверка включена только для 2 товаров владельца (12 ключей) раз в 20 минут; другие пользователи scheduled-нагрузку не создают.
- Geo оставлен в прежней схеме по просьбе пользователя; возможное сокращение глубины и улучшение отображения ошибок — отдельная задача.

## Состояние и память

- Перед продолжением читай `SESSION_STATE.md`, `PROJECT_CONTEXT.md`, `TODO.md` и эту инструкцию.
- `npm run save-session-state` недоступен: в Python-проекте нет `package.json`.
- `KnowledgeBase.tsx` отсутствует, React UI в репозитории нет.
- Production содержит untracked operational files/backups; не удалять без явного разрешения.
