# Wildberries Search API — Руководство

## Endpoint

```
GET https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search
```

## Обязательные параметры запроса

| Параметр    | Значение      | Описание                        |
|-------------|---------------|---------------------------------|
| appType     | 64            | Тип приложения (десктоп)        |
| curr        | rub           | Валюта                          |
| dest        | -951305       | Регион доставки (Москва)        |
| lang        | ru            | Язык                            |
| page        | 1             | Номер страницы (1, 2, 3...)     |
| query       | трусы женские | Поисковый запрос                |
| resultset   | catalog       | Тип выдачи                      |
| sort        | popular       | Сортировка (popular по умолч.)  |
| spp         | 30            | Процент скидки SPP              |

## Заголовки

```python
headers = {
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://www.wildberries.ru/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-fetch-dest": "empty",
}
```

## Антибот-защита: x_wbaas_token

WB требует куку `x_wbaas_token`. Без неё запросы возвращают 403 или пустые данные.

### Как получить токен (через Playwright)

```python
from playwright.sync_api import sync_playwright
import json, time

def get_wbaas_token():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        # Скрыть webdriver-флаг
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = ctx.new_page()
        page.goto("https://www.wildberries.ru/", timeout=30000)
        page.wait_for_timeout(8000)  # ждём JS-загрузку
        cookies = {c["name"]: c["value"] for c in ctx.cookies()}
        browser.close()

    return cookies.get("x_wbaas_token", "")
```

### Использование токена

```python
token = get_wbaas_token()
headers["Cookie"] = f"x_wbaas_token={token}"
```

Токен живёт несколько часов. Рекомендуется кешировать и обновлять при ошибках.

## Опционально: Bearer-токен (авторизация покупателя)

Для персонализированной выдачи можно добавить WB-токен покупателя:

```python
headers["Authorization"] = f"Bearer {wb_user_token}"
# x-userid извлекается из JWT-payload токена (поле "user")
headers["x-userid"] = "12345678"
```

Без Bearer-токена запросы работают, но выдача может отличаться от персонализированной.

## Пример запроса (aiohttp)

```python
import aiohttp
import asyncio

async def search_wb(query: str, page: int = 1):
    headers = {
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://www.wildberries.ru/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Cookie": f"x_wbaas_token={YOUR_TOKEN}",
    }
    params = {
        "appType": "64",
        "curr": "rub",
        "dest": "-951305",
        "lang": "ru",
        "page": page,
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": "30",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(
            "https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search",
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            else:
                print(f"Error: {resp.status}")
                return {}

data = asyncio.run(search_wb("трусы женские"))
```

## Структура ответа

```json
{
  "metadata": { ... },
  "products": [ ... ],
  "total": 12345
}
```

### Поля товара (products[])

| Поле              | Тип     | Описание                                    |
|-------------------|---------|---------------------------------------------|
| id                | int     | Артикул (SKU)                               |
| name              | str     | Название товара                             |
| brand             | str     | Бренд                                       |
| brandId           | int     | ID бренда                                   |
| supplier          | str     | Название продавца                           |
| supplierId        | int     | ID продавца                                 |
| supplierRating    | float   | Рейтинг продавца                            |
| rating            | int     | Рейтинг товара (1-5)                        |
| reviewRating      | float   | Средний рейтинг отзывов                     |
| feedbacks         | int     | Количество отзывов                          |
| sizes             | list    | Размеры + цены (см. ниже)                   |
| totalQuantity     | int     | Общий остаток на складах                    |
| colors            | list    | Цвета: [{name, id}, ...]                    |
| pics              | int     | Количество фотографий                       |
| volume            | int     | Объём упаковки                              |
| weight            | float   | Вес (кг)                                    |
| wh                | int     | ID склада (ближайший)                       |
| dist              | int     | Расстояние доставки (км)                    |
| time1 / time2     | int     | Сроки доставки (дни)                        |
| subjectId         | int     | ID категории товара                         |
| subjectParentId   | int     | ID родительской категории                   |
| entity            | str     | Тип товара (например "трусы")               |
| root              | int     | ID карточки (группирует цвета)              |
| kindId            | int     | Вид товара                                  |
| panelPromoId      | int     | ID промо-акции (0 = нет)                    |
| matchId           | int     | ID совпадения поиска                        |

### Цены (внутри sizes[].price)

```json
{
  "basic": 170000,      // базовая цена в копейках (1700₽)
  "product": 63200,     // цена со скидкой (632₽)
  "logistics": 0,       // стоимость доставки
  "return_": 0          // стоимость возврата
}
```

**Формула:** реальная цена = `product / 100` рублей.

### Размеры (sizes[])

```json
{
  "name": "56",
  "origName": "М1",
  "rank": 694114,
  "optionId": 1036790846,
  "wh": 301809,
  "time1": 1,
  "time2": 28,
  "price": { ... }
}
```

## Позиция товара в выдаче

Позиция вычисляется по индексу в массиве `products`:

```python
position = (page - 1) * 100 + index + 1
```

Где `100` — количество товаров на странице (WB_ITEMS_PER_PAGE).

## Чего НЕТ в этом endpoint

- Описание товара
- Характеристики / состав
- URL фотографий (только количество)
- История цен

Для получения описания и характеристик нужен другой API — карточка товара.

## Важные моменты

1. **100 товаров на страницу** — пагинация через параметр `page`
2. **Не банят по IP** — проблемы обычно из-за отсутствия `x_wbaas_token`
3. **429 Too Many Requests** — при слишком частых запросах, делайте паузу 0.5-1 сек
4. **401 Unauthorized** — Bearer-токен истёк, нужен новый
5. **dest влияет на выдачу** — разные регионы дают разные позиции и остатки
