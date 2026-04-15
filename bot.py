"""Telegram bot for WB Position Parser."""

import asyncio
import html
import logging
import os
import tempfile
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, FSInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    TelegramObject, CopyTextButton,
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config
import db
import parser
import charts
import alerts
import xlsx_loader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

if config.TELEGRAM_PROXY:
    from aiohttp import BasicAuth
    from aiogram.client.session.aiohttp import AiohttpSession
    _proxy_session = AiohttpSession(proxy=config.TELEGRAM_PROXY)
    bot = Bot(token=config.BOT_TOKEN, session=_proxy_session)
    logger.info("Telegram API via proxy: %s", config.TELEGRAM_PROXY.split("@")[-1] if "@" in config.TELEGRAM_PROXY else config.TELEGRAM_PROXY)
else:
    bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
router = Router()


class AuthMiddleware(BaseMiddleware):
    """Block unauthorized users. First /start user becomes owner."""

    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = None
        if isinstance(event, Message):
            user = event.from_user
        elif isinstance(event, CallbackQuery):
            user = event.from_user

        if user:
            if not db.has_any_users():
                # First user becomes owner
                db.add_user(user.id, user.username or user.first_name or "", is_owner=True)
                logger.info(f"Owner set: {user.id} ({user.username})")
            elif not db.is_user_allowed(user.id):
                if isinstance(event, Message):
                    await event.answer(
                        f"⛔ <b>Доступ запрещён</b>\n\n"
                        f"Скопируйте ID и отправьте владельцу бота для получения доступа.",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(
                                text=f"📋 Копировать ID: {user.id}",
                                copy_text=CopyTextButton(text=str(user.id)),
                            )],
                        ]),
                    )
                elif isinstance(event, CallbackQuery):
                    await event.answer("⛔ Доступ запрещён.", show_alert=True)
                return

        return await handler(event, data)


router.message.middleware(AuthMiddleware())
router.callback_query.middleware(AuthMiddleware())
dp.include_router(router)

scheduler = AsyncIOScheduler()
_background_tasks: set = set()


# --- FSM States ---

class AddArticle(StatesGroup):
    waiting_sku = State()

class AddQuery(StatesGroup):
    waiting_query = State()

class SetInterval(StatesGroup):
    waiting_value = State()

class SetDepth(StatesGroup):
    waiting_value = State()

class SetAlertThreshold(StatesGroup):
    waiting_value = State()

class AddToken(StatesGroup):
    waiting_token = State()

class RenameArticle(StatesGroup):
    waiting_name = State()

class AddUser(StatesGroup):
    waiting_id = State()

class AddCompetitor(StatesGroup):
    waiting_sku = State()


# --- Menu button texts (used to prevent FSM handlers from intercepting keyboard presses) ---

MENU_TEXTS = frozenset({
    "Поиск", "Авто", "📈 Графики",
    "📋 Артикулы", "📂 Загрузить XLSX", "🔔 Уведомления",
    "🌍 Гео-сканер", "Полки",
})

# --- Keyboards ---

def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Поиск"), KeyboardButton(text="Полки"), KeyboardButton(text="Авто")],
            [KeyboardButton(text="📈 Графики"), KeyboardButton(text="🌍 Гео-сканер")],
            [KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )



def articles_kb(uid: int, articles_list: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for a in articles_list:
        queries = db.get_queries(uid, a["id"])
        name = a.get("name") or ""
        if name:
            label = f"{a['sku']} — {name} ({len(queries)} запр.)"
        else:
            label = f"{a['sku']} ({len(queries)} запр.)"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"art_{a['id']}")])
    buttons.append([InlineKeyboardButton(text="✏️ Переименовать", callback_data="rename_pick")])
    buttons.append([InlineKeyboardButton(text="➕ Добавить артикул", callback_data="add_article")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def article_actions_kb(article_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Запросы", callback_data=f"queries_{article_id}")],
        [InlineKeyboardButton(text="➕ Добавить запрос", callback_data=f"addq_{article_id}")],
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"rename_{article_id}")],
        [InlineKeyboardButton(text="🗑 Удалить артикул", callback_data=f"delart_{article_id}")],
    ])


def escape(text: str) -> str:
    """Escape HTML special characters."""
    return html.escape(text)


# --- /start ---

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    uid = message.from_user.id
    await state.clear()
    await message.answer(
        "📦 <b>WB Position Parser</b>\n\n"
        "Отслеживаю позиции товаров в поиске Wildberries.\n\n"
        "• Отправь <b>артикул</b> (число) чтобы добавить\n"
        "• Загрузи <b>.xlsx</b> файл с артикулами и запросами\n"
        "• Используй кнопки для управления",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


# --- FSM handlers (must be registered BEFORE the generic number handler) ---

@router.message(SetInterval.waiting_value, ~F.text.in_(MENU_TEXTS))
async def set_interval_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    try:
        value = int(message.text.strip())
        if value < 1:
            raise ValueError
    except ValueError:
        await message.answer("Введи положительное число.")
        return

    db.set_setting(uid, "interval_minutes", str(value))
    reschedule_parser()
    await message.answer(f"✅ Интервал: {value} мин", reply_markup=main_kb())
    await state.clear()


@router.message(SetDepth.waiting_value, ~F.text.in_(MENU_TEXTS))
async def set_depth_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    try:
        value = int(message.text.strip())
        if value < 1 or value > 10:
            raise ValueError
    except ValueError:
        await message.answer("Введи число от 1 до 10.")
        return

    db.set_setting(uid, "pages_depth", str(value))
    await message.answer(f"✅ Глубина: {value} стр. ({value * 100} позиций)", reply_markup=main_kb())
    await state.clear()


@router.message(SetAlertThreshold.waiting_value, ~F.text.in_(MENU_TEXTS))
async def set_threshold_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    try:
        value = int(message.text.strip())
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("Введи положительное число.")
        return

    data = await state.get_data()
    db.set_alert_threshold(uid, data["alert_type"], value)
    await message.answer(f"✅ Порог установлен: {value}", reply_markup=main_kb())
    await state.clear()


@router.message(AddArticle.waiting_sku, ~F.text.in_(MENU_TEXTS))
async def add_article_fsm(message: Message, state: FSMContext):
    uid = message.from_user.id
    sku = message.text.strip()
    if not sku.isdigit() or len(sku) < 5:
        await message.answer("Введи артикул (число, минимум 5 цифр).")
        return
    try:
        await message.delete()
    except Exception:
        pass
    result = db.add_article(uid, sku)
    article = db.get_article_by_sku(uid, sku)
    art_id = article["id"] if article else 0
    if result:
        await message.answer(
            f"✅ Артикул <b>{sku}</b> добавлен.",
            parse_mode="HTML",
            reply_markup=article_actions_kb(art_id),
        )
    else:
        await message.answer(
            f"Артикул {sku} уже существует.",
            reply_markup=article_actions_kb(art_id),
        )
    await state.clear()


@router.message(AddToken.waiting_token, ~F.text.in_(MENU_TEXTS))
async def add_token_process(message: Message, state: FSMContext):
    token = message.text.strip()
    # Delete the message with token for security
    try:
        await message.delete()
    except Exception:
        pass

    if len(token) < 50:
        await message.answer("Токен слишком короткий. Попробуй ещё раз.")
        return

    label = f"Token #{len(db.get_wb_tokens()) + 1}"
    result = db.add_wb_token(token, label)
    if result:
        await message.answer(
            f"✅ Токен добавлен: <b>{label}</b>\n"
            f"Первые символы: <code>{token[:20]}...</code>",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    else:
        await message.answer("Ошибка при добавлении токена.", reply_markup=main_kb())
    await state.clear()


@router.message(AddQuery.waiting_query, ~F.text.in_(MENU_TEXTS))
async def add_query_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    art_id = data.get("art_id")
    article = db.get_article_by_id(uid, art_id) if art_id else None
    if not article:
        await message.answer("Артикул не найден.")
        await state.clear()
        return

    sku = article["sku"]
    lines = [line.strip() for line in message.text.split("\n") if line.strip()]
    added = 0
    for line in lines:
        if db.add_query(uid, article["id"], line):
            added += 1

    await message.answer(
        f"✅ Добавлено {added} запросов для {escape(sku)}.",
        parse_mode="HTML",
        reply_markup=article_actions_kb(article["id"]),
    )
    await state.clear()


# --- Articles ---

@router.message(F.text == "📋 Артикулы")
async def show_articles(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    try:
        arts = db.get_articles(uid)
        if not arts:
            await message.answer(
                "Артикулов нет.\n\nОтправь артикул (число) чтобы добавить,\n"
                "или загрузи XLSX файл.",
            )
            return
        text = "📋 <b>Артикулы:</b>\n\n"
        for a in arts:
            queries = db.get_queries(uid, a["id"])
            name = a.get("name") or ""
            if name:
                text += f"• <b>{a['sku']}</b> — {escape(name)} ({len(queries)} запросов)\n"
            else:
                text += f"• <b>{a['sku']}</b> — {len(queries)} запросов\n"
        text += "\n✏️ <i>Переименовать</i> — добавить название артикулу"
        await message.answer(text, parse_mode="HTML", reply_markup=articles_kb(uid, arts))
    except Exception as e:
        logger.error(f"show_articles error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


@router.callback_query(F.data == "add_article")
async def add_article_start(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    await state.set_state(AddArticle.waiting_sku)
    await callback.message.answer("Введи артикул (число):")
    await callback.answer()


@router.callback_query(F.data.startswith("art_"))
async def select_article(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("art_", ""))
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return
    sku = article["sku"]
    name = article.get("name") or ""
    queries = db.get_queries(uid, article["id"])
    text = f"📦 Артикул: <b>{escape(sku)}</b>\n"
    if name:
        text += f"📝 Название: <b>{escape(name)}</b>\n"
    text += f"📋 Запросов: {len(queries)}"
    if queries:
        text += "\n\n"
        for q in queries[:20]:
            text += f"• {escape(q['query'])}\n"
        if len(queries) > 20:
            text += f"... и ещё {len(queries) - 20}\n"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=article_actions_kb(art_id))


@router.callback_query(F.data == "rename_pick")
async def rename_pick(callback: CallbackQuery):
    uid = callback.from_user.id
    await callback.answer()
    arts = db.get_articles(uid)
    if not arts:
        await callback.message.edit_text("Нет артикулов для переименования.")
        return
    buttons = []
    for a in arts:
        name = a.get("name") or ""
        label = f"{a['sku']} — {name}" if name else a['sku']
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"rename_{a['id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_articles")])
    await callback.message.edit_text(
        "✏️ Выбери артикул для переименования:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("rename_"))
async def rename_article_start(callback: CallbackQuery, state: FSMContext):
    art_id = int(callback.data.replace("rename_", ""))
    await state.set_state(RenameArticle.waiting_name)
    await state.update_data(article_id=art_id)
    await callback.message.answer("✏️ Введи название для этого артикула:")
    await callback.answer()


@router.message(RenameArticle.waiting_name, ~F.text.in_(MENU_TEXTS))
async def rename_article_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    name = message.text.strip()
    if len(name) > 100:
        await message.answer("Слишком длинное название (макс. 100 символов).")
        return
    data = await state.get_data()
    art_id = data["article_id"]
    db.update_article_name(uid, art_id, name)
    article = db.get_article_by_id(uid, art_id)
    sku = article["sku"] if article else "?"
    await message.answer(
        f"✅ Артикул <b>{sku}</b> переименован в <b>{escape(name)}</b>",
        parse_mode="HTML",
        reply_markup=article_actions_kb(art_id),
    )
    await state.clear()


# Add article by sending a number (only when NOT in FSM state)
@router.message(F.text.regexp(r"^\d{5,15}$"), StateFilter(None))
async def add_article_by_number(message: Message):
    uid = message.from_user.id
    sku = message.text.strip()
    result = db.add_article(uid, sku)
    article = db.get_article_by_sku(uid, sku)
    art_id = article["id"] if article else 0
    if result:
        await message.answer(
            f"✅ Артикул <b>{sku}</b> добавлен.\n\n"
            f"Теперь добавь поисковые запросы или загрузи XLSX.",
            parse_mode="HTML",
            reply_markup=article_actions_kb(art_id),
        )
    else:
        await message.answer(
            f"Артикул {sku} уже существует.",
            reply_markup=article_actions_kb(art_id),
        )


@router.callback_query(F.data.startswith("delart_"))
async def delete_article(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("delart_", ""))
    article = db.get_article_by_id(uid, art_id)
    sku = article["sku"] if article else "?"
    db.remove_article_by_id(uid, art_id)
    await callback.message.edit_text(f"🗑 Артикул {escape(sku)} удалён.", parse_mode="HTML")
    await callback.answer()


# --- Queries ---

def _build_queries_view(uid: int, art_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build text and keyboard for queries list."""
    article = db.get_article_by_id(uid, art_id)
    sku = article["sku"] if article else "?"
    queries = db.get_queries(uid, art_id) if article else []

    if not queries:
        return (
            f"📋 <b>Запросы для {escape(sku)}:</b>\n\nПусто",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить запрос", callback_data=f"addq_{art_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"art_{art_id}")],
            ]),
        )

    text = f"📋 <b>Запросы для {escape(sku)}:</b>\n\n"
    for i, q in enumerate(queries):
        text += f"{i+1}. {escape(q['query'])}\n"

    buttons = []
    for i, q in enumerate(queries):
        row = []
        if i > 0:
            row.append(InlineKeyboardButton(text=f"⬆️ {i+1}", callback_data=f"qup_{q['id']}_{art_id}"))
        row.append(InlineKeyboardButton(text=f"🗑 {i+1}", callback_data=f"delq_{q['id']}_{art_id}"))
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data=f"addq_{art_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"art_{art_id}")])

    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("queries_"))
async def show_queries(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("queries_", ""))
    if not db.get_article_by_id(uid, art_id):
        await callback.answer("Артикул не найден")
        return

    text, kb = _build_queries_view(uid, art_id)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await bot.send_message(callback.message.chat.id, text, parse_mode="HTML", reply_markup=kb)


async def _refresh_queries(callback: CallbackQuery, art_id: int):
    """Delete old message and send fresh queries list."""
    uid = callback.from_user.id
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass

    text, kb = _build_queries_view(uid, art_id)
    await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("qup_"))
async def move_query_up(callback: CallbackQuery):
    uid = callback.from_user.id
    parts = callback.data.split("_")
    query_id = int(parts[1])
    art_id = int(parts[2])
    db.swap_query_order(uid, query_id, "up")
    await callback.answer("Перемещено")
    await _refresh_queries(callback, art_id)


@router.callback_query(F.data.startswith("delq_"))
async def delete_query(callback: CallbackQuery):
    uid = callback.from_user.id
    parts = callback.data.split("_")
    query_id = int(parts[1])
    art_id = int(parts[2])
    db.remove_query(uid, query_id)
    await callback.answer("Запрос удалён")
    await _refresh_queries(callback, art_id)


@router.callback_query(F.data.startswith("addq_"))
async def add_query_start(callback: CallbackQuery, state: FSMContext):
    art_id = int(callback.data.replace("addq_", ""))
    article = db.get_article_by_id(callback.from_user.id, art_id)
    sku = article["sku"] if article else "?"
    await state.set_state(AddQuery.waiting_query)
    await state.update_data(sku=sku, art_id=art_id)
    await callback.message.answer(
        f"Введи поисковые запросы для <b>{escape(sku)}</b>.\n"
        "Каждый запрос с новой строки:",
        parse_mode="HTML",
    )
    await callback.answer()


# --- XLSX Upload ---

@router.message(F.text == "📂 Загрузить XLSX")
async def xlsx_prompt(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    await message.answer(
        "📂 Отправь <b>.xlsx</b> файл.\n\n"
        "Формат:\n"
        "• Столбец A — артикул\n"
        "• Столбец B — поисковый запрос",
        parse_mode="HTML",
    )


@router.message(F.document)
async def handle_document(message: Message):
    uid = message.from_user.id
    doc = message.document
    if not doc.file_name or not doc.file_name.endswith(".xlsx"):
        await message.answer("Нужен файл .xlsx")
        return

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        await bot.download(doc, destination=tmp_path)
        result = xlsx_loader.load_from_xlsx(uid, tmp_path)

        text = "📂 <b>Загрузка завершена:</b>\n\n"
        text += f"✅ Добавлено: {len(result['added'])}\n"
        text += f"⏭ Пропущено (дубли): {len(result['skipped'])}\n"

        if result["added"]:
            skus = sorted(set(sku for sku, _ in result["added"]))
            text += f"\nАртикулы: {', '.join(skus)}"

        await message.answer(text, parse_mode="HTML", reply_markup=main_kb())
    except Exception as e:
        logger.error(f"XLSX load error: {e}")
        await message.answer("Ошибка при загрузке файла.")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# --- Parsing ---

@router.message(F.text == "Поиск")
async def parse_menu(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    try:
        arts = db.get_articles(uid)
        if not arts:
            await message.answer("Нет артикулов для проверки.", reply_markup=main_kb())
            return

        buttons = []
        for a in arts:
            qcount = len(db.get_queries(uid, a["id"]))
            buttons.append([InlineKeyboardButton(
                text=f"🔍 {a['sku'] + ' — ' + a['name'] if a.get('name') else a['sku']} ({qcount} запр.)",
                callback_data=f"evirma_{a['id']}",
            )])
        buttons.append([InlineKeyboardButton(text="🔍 Проверить ВСЕ", callback_data="evirma_all")])

        await message.answer("Выбери артикул для проверки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"parse_menu error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


@router.callback_query(F.data.startswith("parsepos_"))
async def run_parse_handler(callback: CallbackQuery):
    uid = callback.from_user.id
    target = callback.data.replace("parsepos_", "")
    await callback.answer()

    # Run parsing in background so bot stays responsive
    chat_id = callback.message.chat.id
    msg_id = callback.message.message_id

    try:
        if target == "all":
            await callback.message.edit_text("⏳ Проверяю все артикулы...")
            task = asyncio.create_task(_do_parse_all(uid, chat_id, msg_id))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
        else:
            art_id = int(target)
            article = db.get_article_by_id(uid, art_id)
            if not article:
                await callback.message.edit_text("Артикул не найден.")
                return
            sku = article["sku"]
            queries = db.get_queries(uid, article["id"])
            if not queries:
                await callback.message.edit_text(f"У артикула {escape(sku)} нет запросов.", parse_mode="HTML")
                return

            pages = int(db.get_setting(uid, "pages_depth") or 3)
            await callback.message.edit_text(
                f"⏳ Проверяю <b>{sku}</b> ({len(queries)} запросов, глубина {pages} стр.)...",
                parse_mode="HTML",
            )
            task = asyncio.create_task(_do_parse_one(uid, chat_id, msg_id, article, queries, pages))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
    except Exception as e:
        logger.error(f"run_parse_handler error: {e}")
        try:
            await callback.message.edit_text(f"⚠️ Ошибка: {e}")
        except Exception:
            pass


async def _do_parse_one(uid: int, chat_id: int, msg_id: int, article: dict, queries: list, pages: int):
    """Background task: parse one article."""
    try:
        sku = article["sku"]
        start = time.time()
        results = await parser.run_parse(uid, article["id"], sku, queries, pages)
        elapsed = time.time() - start
        art_name = article.get("name") or ""
        text = format_results(sku, results, elapsed, name=art_name)
        await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")

        alert_msgs = alerts.check_alerts(uid, article["id"], sku, results)
        for msg in alert_msgs:
            await bot.send_message(chat_id, msg)
    except Exception as e:
        logger.error(f"Parse error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


async def _do_parse_all(uid: int, chat_id: int, msg_id: int):
    """Background task: parse all articles."""
    try:
        start = time.time()
        all_results = await parser.run_full_parse(uid)
        elapsed = time.time() - start
        if not all_results:
            await bot.edit_message_text("Нет данных для проверки.", chat_id=chat_id, message_id=msg_id)
            return

        text = format_results_all(all_results, elapsed, uid=uid)
        if len(text) <= 4096:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
        else:
            await bot.edit_message_text(f"{datetime.now().strftime('%H:%M %d.%m')}{_elapsed_str(elapsed)}", chat_id=chat_id, message_id=msg_id)
            for sku, results in all_results.items():
                art = db.get_article_by_sku(uid, sku)
                art_name = art.get("name", "") if art else ""
                part = format_results(sku, results, name=art_name)
                await bot.send_message(chat_id, part, parse_mode="HTML")

        for sku, results in all_results.items():
            article = db.get_article_by_sku(uid, sku)
            if article:
                alert_msgs = alerts.check_alerts(uid, article["id"], sku, results)
                for a_msg in alert_msgs:
                    await bot.send_message(chat_id, a_msg)
    except Exception as e:
        logger.error(f"Parse all error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


def _elapsed_str(elapsed: float = None) -> str:
    if elapsed is None:
        return ""
    if elapsed < 60:
        return f" | {elapsed:.0f} сек"
    return f" | {int(elapsed // 60)}м {int(elapsed % 60)}с"


def _format_sku_block(sku: str, results: list[dict], name: str = "") -> tuple[list[str], int]:
    """Build lines for one SKU block. Returns (lines, error_count)."""
    header = f"{sku} — {name}" if name else sku
    # Сначала собираем строки запросов
    query_lines = []
    errors = 0
    for r in results:
        query = r["query"]
        pos = r["position"]
        if r.get("error"):
            pos_str = "ERR"
            errors += 1
        elif pos is not None:
            pos_str = str(pos)
        else:
            pos_str = "—"
        padding = max(1, 32 - len(query))
        query_lines.append(f"{query}{' ' * padding}│ {pos_str}")
    # Центрируем заголовок по ширине самой длинной строки
    max_width = max((len(line) for line in query_lines), default=38)
    header = header.center(max_width)
    lines = [header, ""] + query_lines
    return lines, errors


def format_results(sku: str, results: list[dict], elapsed: float = None, name: str = "") -> str:
    """Format parse results for single SKU."""
    now = datetime.now().strftime("%H:%M %d.%m")
    text = f"<b>SKU {sku}</b> | {now}{_elapsed_str(elapsed)}\n\n"

    lines, errors = _format_sku_block(sku, results, name)
    text += f"<pre>{chr(10).join(lines)}</pre>"

    if errors:
        text += f"\n⚠️ {errors} запросов не выполнены (WB не ответил)"
    return text


def format_results_all(all_results: dict, elapsed: float = None, uid: int = None) -> str:
    """Format results for ALL articles in one message."""
    now = datetime.now().strftime("%H:%M %d.%m")
    text = f"{now}{_elapsed_str(elapsed)}\n\n"

    all_lines = []
    total_errors = 0
    skus = list(all_results.keys())

    for idx, (sku, results) in enumerate(all_results.items()):
        name = ""
        if uid:
            art = db.get_article_by_sku(uid, sku)
            name = art.get("name", "") if art else ""
        lines, errors = _format_sku_block(sku, results, name)
        all_lines.extend(lines)
        total_errors += errors
        if idx < len(skus) - 1:
            all_lines.append("────────────────────────────┴──────┴──────┘")

    text += f"<pre>{chr(10).join(all_lines)}</pre>"

    if total_errors:
        text += f"\n⚠️ {total_errors} запросов не выполнены (WB не ответил)"
    return text


# --- Organic/Promo positions via Chrome ---

import chrome_positions
from queue_worker import position_queue


@router.callback_query(F.data.startswith("evirma_"))
async def run_evirma_handler(callback: CallbackQuery):
    uid = callback.from_user.id
    target = callback.data.replace("evirma_", "")
    await callback.answer()

    chat_id = callback.message.chat.id
    msg_id = callback.message.message_id

    try:
        if target == "all":
            await callback.message.edit_text("⏳ Проверяю все артикулы (Орг/Рекл)...")
            task = asyncio.create_task(_do_evirma_all(uid, chat_id, msg_id))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
        else:
            art_id = int(target)
            article = db.get_article_by_id(uid, art_id)
            if not article:
                await callback.message.edit_text("Артикул не найден.")
                return
            sku = article["sku"]
            queries = db.get_queries(uid, article["id"])
            if not queries:
                await callback.message.edit_text(f"У артикула {escape(sku)} нет запросов.", parse_mode="HTML")
                return

            await callback.message.edit_text(
                f"⏳ Проверяю <b>{sku}</b> (Орг/Рекл, {len(queries)} запросов)...",
                parse_mode="HTML",
            )
            task = asyncio.create_task(_do_evirma_one(uid, chat_id, msg_id, article, queries))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
    except Exception as e:
        logger.error(f"run_evirma_handler error: {e}")
        try:
            await callback.message.edit_text(f"⚠️ Ошибка: {e}")
        except Exception:
            pass


async def _do_evirma_one(uid: int, chat_id: int, msg_id: int, article: dict, queries: list):
    """Background task: get evirma positions for one article via queue."""
    try:
        sku = article["sku"]
        art_name = article.get("name") or ""
        nm_id = int(sku) if sku.isdigit() else 0

        if not nm_id:
            await bot.edit_message_text(f"⚠️ SKU {sku} не числовой — evirma требует nm_id.", chat_id=chat_id, message_id=msg_id)
            return

        keywords = [q["query"] for q in queries]

        # Show queue position if there are pending tasks
        pending = position_queue.pending_count
        if pending > 0:
            pos, est = position_queue.queue_info(uid)
            await bot.edit_message_text(
                f"⏳ <b>{sku}</b> в очереди (позиция {pos}, ~{est:.0f} сек)...",
                chat_id=chat_id, message_id=msg_id, parse_mode="HTML"
            )

        start = time.time()
        future = await position_queue.submit(uid, nm_id, keywords, label=sku)
        positions = await future
        elapsed = time.time() - start

        # Save positions for charts
        for q in queries:
            pos_data = positions.get(q["query"], {})
            promo_pos = pos_data.get("promo_pos")
            db.save_result(uid, article["id"], q["id"], promo_pos, 1 if promo_pos and promo_pos <= 300 else 2)

        text = _format_evirma_results(sku, keywords, positions, elapsed, name=art_name)
        await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Evirma parse error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


async def _do_evirma_all(uid: int, chat_id: int, msg_id: int):
    """Background task: evirma positions for all articles via queue."""
    try:
        start = time.time()
        arts = db.get_articles(uid)

        # Submit all tasks to queue
        task_infos = []  # (article, queries, future)
        for article in arts:
            sku = article["sku"]
            nm_id = int(sku) if sku.isdigit() else 0
            if not nm_id:
                continue
            queries = db.get_queries(uid, article["id"])
            if not queries:
                continue
            keywords = [q["query"] for q in queries]
            future = await position_queue.submit(uid, nm_id, keywords, label=sku)
            task_infos.append((article, queries, keywords, future))

        if not task_infos:
            await bot.edit_message_text("Нет данных.", chat_id=chat_id, message_id=msg_id)
            return

        total = len(task_infos)
        await bot.edit_message_text(
            f"⏳ Проверяю {total} артикулов (очередь: {position_queue.pending_count})...",
            chat_id=chat_id, message_id=msg_id
        )

        # Collect results as they complete
        all_blocks = []
        for i, (article, queries, keywords, future) in enumerate(task_infos):
            sku = article["sku"]
            art_name = article.get("name") or ""

            # Update status periodically
            if i > 0 and i % 2 == 0:
                try:
                    await bot.edit_message_text(
                        f"⏳ Проверяю {i+1}/{total}...",
                        chat_id=chat_id, message_id=msg_id
                    )
                except Exception:
                    pass

            positions = await future

            lines = _format_evirma_block(sku, keywords, positions, name=art_name)
            all_blocks.append(lines)

            # Save positions for charts
            for q in queries:
                pos_data = positions.get(q["query"], {})
                promo_pos = pos_data.get("promo_pos")
                db.save_result(uid, article["id"], q["id"], promo_pos, 1 if promo_pos and promo_pos <= 300 else 2)

        elapsed = time.time() - start
        now = datetime.now().strftime("%H:%M %d.%m")

        if not all_blocks:
            await bot.edit_message_text("Нет данных.", chat_id=chat_id, message_id=msg_id)
            return

        separator = ["────────────────────────────┴──────┴──────┘"]
        all_lines = []
        for i, block in enumerate(all_blocks):
            all_lines.extend(block)
            if i < len(all_blocks) - 1:
                all_lines.extend(separator); all_lines.append("")

        text = f"{now} | {elapsed:.1f}с\n\n<pre>{chr(10).join(all_lines)}</pre>"

        if len(text) > 4000:
            # Split into chunks
            for i, block in enumerate(all_blocks):
                block_text = f"<pre>{chr(10).join(block)}</pre>"
                if i == 0:
                    await bot.edit_message_text(block_text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
                else:
                    await bot.send_message(chat_id, block_text, parse_mode="HTML")
        else:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Evirma all error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


def _format_evirma_block(sku: str, keywords: list[str], positions: dict, name: str = "") -> list[str]:
    """Build lines for one SKU block with organic + promo positions."""
    header = f"{sku} — {name}" if name else sku
    query_lines = []

    for kw in keywords:
        pos_data = positions.get(kw, {})
        promo = pos_data.get("promo_pos")
        organic = pos_data.get("organic_pos")
        is_ad = pos_data.get("is_advertised", False)
        is_error = pos_data.get("error", False)

        q_display = kw if len(kw) <= 26 else kw[:25] + "…"
        padding = max(1, 28 - len(q_display))

        if is_ad:
            promo_str = str(promo) if promo is not None else "—"
            if organic is not None and promo is not None:
                organic_str = "+" + str(organic - promo)
            else:
                organic_str = "—"
        else:
            promo_str = str(promo) if promo is not None else "—"
            organic_str = "—"

        query_lines.append(f"{q_display}{' ' * padding}│ {promo_str:>4} │ {organic_str:>4} │")

    col_header = f"{'':28}│  поз │ буст │"
    col_sep    = f"{'':28}│------+------│"
    lines = [header, col_header, col_sep] + query_lines
    return lines


def _format_evirma_results(sku: str, keywords: list[str], positions: dict, elapsed: float = None, name: str = "") -> str:
    """Format evirma results for single SKU."""
    now = datetime.now().strftime("%H:%M %d.%m")
    lines = _format_evirma_block(sku, keywords, positions, name)
    text = f"<b>SKU {sku}</b> | {now}{_elapsed_str(elapsed)}\n\n"
    text += f"<pre>{chr(10).join(lines)}</pre>"
    return text


# --- Auto Check ---

@router.message(F.text == "Авто")
async def auto_menu(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    try:
        arts = db.get_articles(uid)
        if not arts:
            await message.answer("Нет артикулов.", reply_markup=main_kb())
            return

        interval = db.get_setting(uid, "interval_minutes")
        auto_arts = [a for a in arts if a.get("auto_check")]

        text = f"🔄 <b>Автопроверка</b>\n\n"
        text += f"⏱ Интервал: <b>{interval} мин</b>\n"
        text += f"📦 В авто: <b>{len(auto_arts)}</b> из {len(arts)}\n"

        buttons = _auto_buttons(uid, arts)
        await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"auto_menu error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


def _auto_buttons(uid, arts):
    buttons = []
    for a in arts:
        queries = db.get_queries(uid, a["id"])
        status = "✅" if a.get("auto_check") else "❌"
        buttons.append([InlineKeyboardButton(
            text=f"{status} {a['sku'] + ' — ' + a['name'] if a.get('name') else a['sku']} ({len(queries)} запр.)",
            callback_data=f"auto_toggle_{a['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="⏱ Интервал", callback_data="set_interval")])
    return buttons


@router.callback_query(F.data.startswith("auto_toggle_"))
async def auto_toggle(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("auto_toggle_", ""))
    try:
        new_state = db.toggle_auto_check(uid, art_id)
        article = db.get_article_by_id(uid, art_id)
        sku = article["sku"] if article else "?"
        await callback.answer(f"{sku}: {'включён' if new_state else 'выключен'}")

        arts = db.get_articles(uid)
        interval = db.get_setting(uid, "interval_minutes")
        auto_arts = [a for a in arts if a.get("auto_check")]
        text = f"🔄 <b>Автопроверка</b>\n\n⏱ Интервал: <b>{interval} мин</b>\n📦 В авто: <b>{len(auto_arts)}</b> из {len(arts)}\n"
        buttons = _auto_buttons(uid, arts)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"auto_toggle error: {e}")
        await callback.answer(f"Ошибка: {e}", show_alert=True)


# --- Charts ---

@router.message(F.text == "📈 Графики")
async def charts_menu(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    try:
        arts = db.get_articles(uid)
        if not arts:
            await message.answer("Нет артикулов.", reply_markup=main_kb())
            return
        buttons = []
        for a in arts:
            buttons.append([InlineKeyboardButton(
                text=f"📈 {a['sku'] + ' — ' + a['name'] if a.get('name') else a['sku']}",
                callback_data=f"chart_{a['id']}",
            )])
        await message.answer("Выбери артикул:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"charts_menu error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


@router.callback_query(F.data.startswith("chart_"))
async def show_chart(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("chart_", ""))
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return
    sku = article["sku"]

    await callback.answer("Генерирую график...")

    try:
        path = await asyncio.to_thread(charts.generate_article_chart, uid, article["id"], sku, 7)
        if path and os.path.exists(path):
            await callback.message.answer_photo(
                FSInputFile(path),
                caption=f"📈 Динамика позиций: {sku} (7 дней)",
            )
        else:
            await callback.message.answer("Недостаточно данных для графика (нужно минимум 2 проверки).")
    except Exception as e:
        logger.error(f"show_chart error: {e}")
        await callback.message.answer(f"⚠️ Ошибка генерации графика: {e}")


# --- Settings ---

@router.message(F.text == "⚙️ Настройки")
async def show_settings(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    try:
        interval = db.get_setting(uid, "interval_minutes")
        depth = db.get_setting(uid, "pages_depth")

        text = (
            "⚙️ <b>Настройки</b>\n\n"
        )

        if db.is_owner(uid):
            tokens = db.get_wb_tokens()
            active_tokens = [t for t in tokens if t["is_active"]]
            text += f"🔑 Токенов WB: <b>{len(active_tokens)}</b> активных из {len(tokens)}\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Поиск", callback_data="settings_search"),
             InlineKeyboardButton(text="🏪 Полки", callback_data="settings_shelves")],
            [InlineKeyboardButton(text="🔔 Уведомления", callback_data="go_alerts")],
        ])
        if db.is_owner(uid):
            kb.inline_keyboard.append([InlineKeyboardButton(text="🔑 Токены WB", callback_data="tokens_menu")])
            kb.inline_keyboard.append([InlineKeyboardButton(text="👥 Пользователи", callback_data="users_menu")])
        await message.answer(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.error(f"show_settings error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


@router.callback_query(F.data == "settings_search")
async def settings_search(callback: CallbackQuery):
    """Settings → Search: articles + queries management."""
    uid = callback.from_user.id
    await callback.answer()
    arts = db.get_articles(uid)
    if not arts:
        await callback.message.edit_text(
            "🔍 <b>Настройки поиска</b>\n\nАртикулов нет. Добавь первый.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить артикул", callback_data="add_article")],
                [InlineKeyboardButton(text="📂 Загрузить XLSX", callback_data="go_xlsx")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_settings")],
            ]),
        )
        return
    text = "🔍 <b>Настройки поиска</b>\n\n"
    for a in arts:
        queries = db.get_queries(uid, a["id"])
        name = a.get("name") or ""
        if name:
            text += f"• <b>{a['sku']}</b> — {escape(name)} ({len(queries)} запросов)\n"
        else:
            text += f"• <b>{a['sku']}</b> — {len(queries)} запросов\n"
    buttons = []
    for a in arts:
        queries = db.get_queries(uid, a["id"])
        name = a.get("name") or ""
        label = f"{a['sku']} — {name} ({len(queries)} запр.)" if name else f"{a['sku']} ({len(queries)} запр.)"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"art_{a['id']}")])
    buttons.append([InlineKeyboardButton(text="✏️ Переименовать", callback_data="rename_pick")])
    buttons.append([InlineKeyboardButton(text="➕ Добавить артикул", callback_data="add_article")])
    buttons.append([InlineKeyboardButton(text="📂 Загрузить XLSX", callback_data="go_xlsx")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_settings")])
    await callback.message.edit_text(text, parse_mode="HTML",
                                      reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "settings_shelves")
async def settings_shelves(callback: CallbackQuery):
    """Settings → Shelves: articles + competitors management."""
    uid = callback.from_user.id
    await callback.answer()
    arts = db.get_articles(uid)
    if not arts:
        await callback.message.edit_text(
            "🏪 <b>Настройки полок</b>\n\nАртикулов нет. Сначала добавь артикулы в настройках поиска.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_settings")],
            ]),
        )
        return
    text = "🏪 <b>Настройки полок</b>\n\n"
    buttons = []
    for a in arts:
        comp_count = db.count_competitors(uid, a["id"])
        name = a.get("name") or ""
        label = f"{a['sku']}"
        if name:
            label += f" — {name}"
        label += f" ({comp_count} конк.)"
        text += f"• <b>{a['sku']}</b>"
        if name:
            text += f" — {escape(name)}"
        text += f" ({comp_count} конкурентов)\n"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"shelf_{a['id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_settings")])
    await callback.message.edit_text(text, parse_mode="HTML",
                                      reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "go_settings")
async def go_settings(callback: CallbackQuery):
    """Return to main settings menu."""
    uid = callback.from_user.id
    await callback.answer()
    text = "⚙️ <b>Настройки</b>\n\n"
    if db.is_owner(uid):
        tokens = db.get_wb_tokens()
        active_tokens = [t for t in tokens if t["is_active"]]
        text += f"🔑 Токенов WB: <b>{len(active_tokens)}</b> активных из {len(tokens)}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="settings_search"),
         InlineKeyboardButton(text="🏪 Полки", callback_data="settings_shelves")],
        [InlineKeyboardButton(text="🔔 Уведомления", callback_data="go_alerts")],
    ])
    if db.is_owner(uid):
        kb.inline_keyboard.append([InlineKeyboardButton(text="🔑 Токены WB", callback_data="tokens_menu")])
        kb.inline_keyboard.append([InlineKeyboardButton(text="👥 Пользователи", callback_data="users_menu")])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "go_articles")
async def go_articles(callback: CallbackQuery):
    uid = callback.from_user.id
    await callback.answer()
    arts = db.get_articles(uid)
    if not arts:
        await callback.message.edit_text(
            "📋 <b>Артикулы:</b>\n\nПусто. Добавь первый артикул.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить артикул", callback_data="add_article")],
                [InlineKeyboardButton(text="📂 Загрузить XLSX", callback_data="go_xlsx")],
            ]),
        )
        return
    text = "📋 <b>Артикулы:</b>\n\n"
    for a in arts:
        queries = db.get_queries(uid, a["id"])
        name = a.get("name") or ""
        if name:
            text += f"• <b>{a['sku']}</b> — {escape(name)} ({len(queries)} запросов)\n"
        else:
            text += f"• <b>{a['sku']}</b> — {len(queries)} запросов\n"
    text += "\n✏️ <i>Переименовать</i> — добавить название артикулу"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=articles_kb(uid, arts))


@router.callback_query(F.data == "go_xlsx")
async def go_xlsx(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📂 Отправь <b>.xlsx</b> файл.\n\n"
        "Формат:\n"
        "• Столбец A — артикул\n"
        "• Столбец B — поисковый запрос",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "go_alerts")
async def go_alerts(callback: CallbackQuery):
    uid = callback.from_user.id
    await callback.answer()
    text, buttons = _build_alerts_view(uid)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


def _build_alerts_view(uid):
    alert_list = db.get_alerts(uid)
    text = "🔔 <b>Уведомления</b>\n\n"
    buttons = []
    for a in alert_list:
        name = ALERT_NAMES.get(a["alert_type"], a["alert_type"])
        short = ALERT_SHORT.get(a["alert_type"], a["alert_type"])
        on = a["enabled"]
        status = "✅ Вкл" if on else "❌ Выкл"
        threshold = a["threshold"]

        if a["alert_type"] == "position_drop_below":
            text += f"{status} | {name}\n"
            text += f"     Порог: <b>{threshold}</b> (уведомление если позиция &gt; {threshold})\n\n"
        elif a["alert_type"] == "disappeared":
            text += f"{status} | {name}\n\n"
        elif a["alert_type"] == "position_change":
            text += f"{status} | {name}\n"
            text += f"     Порог: <b>±{threshold}</b> позиций\n\n"

        toggle_icon = "❌" if on else "✅"
        buttons.append([
            InlineKeyboardButton(text=f"{toggle_icon} {short}", callback_data=f"alert_toggle_{a['alert_type']}"),
            InlineKeyboardButton(text="⚙️ Порог", callback_data=f"alert_thresh_{a['alert_type']}"),
        ])
    return text, buttons


@router.callback_query(F.data == "set_interval")
async def set_interval_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetInterval.waiting_value)
    await callback.message.answer("Введи интервал автопроверки в минутах (число):")
    await callback.answer()


@router.callback_query(F.data == "set_depth")
async def set_depth_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetDepth.waiting_value)
    await callback.message.answer("Введи глубину поиска (кол-во страниц, 1-10):")
    await callback.answer()


# --- WB Tokens ---

@router.callback_query(F.data == "tokens_menu")
async def tokens_menu(callback: CallbackQuery):
    tokens = db.get_wb_tokens()
    text = "🔑 <b>Токены WB (WBTokenV3)</b>\n\n"

    if not tokens:
        text += "Нет токенов. Добавь токен авторизованного пользователя WB."
    else:
        for t in tokens:
            status = "✅" if t["is_active"] else "❌"
            preview = t["token"][:15] + "..."
            label = t.get("label") or f"#{t['id']}"
            text += f"{status} <b>{label}</b>\n"
            text += f"   <code>{preview}</code>\n"
            if t.get("last_error"):
                text += f"   Ошибка: {t['last_error']}\n"
            text += "\n"

    buttons = [[InlineKeyboardButton(text="➕ Добавить токен", callback_data="token_add")]]
    for t in tokens:
        label = t.get("label") or f"#{t['id']}"
        row = []
        if t["is_active"]:
            row.append(InlineKeyboardButton(text=f"❌ Выкл {label}", callback_data=f"token_off_{t['id']}"))
        else:
            row.append(InlineKeyboardButton(text=f"✅ Вкл {label}", callback_data=f"token_on_{t['id']}"))
        row.append(InlineKeyboardButton(text=f"🗑 Удалить", callback_data=f"token_del_{t['id']}"))
        buttons.append(row)

    await callback.message.edit_text(text, parse_mode="HTML",
                                      reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data == "token_add")
async def token_add_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddToken.waiting_token)
    await callback.message.answer(
        "🔑 Отправь токен <b>WBTokenV3</b> от авторизованного пользователя WB.\n\n"
        "Как получить:\n"
        "1. Открой wildberries.ru (залогинься)\n"
        "2. F12 → Application → Cookies\n"
        "3. Скопируй значение <b>WBTokenV3</b>\n\n"
        "Сообщение с токеном будет удалено автоматически.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("token_on_"))
async def token_activate(callback: CallbackQuery):
    token_id = int(callback.data.replace("token_on_", ""))
    db.set_wb_token_active(token_id, True)
    await callback.answer("Токен активирован")
    await tokens_menu(callback)


@router.callback_query(F.data.startswith("token_off_"))
async def token_deactivate(callback: CallbackQuery):
    token_id = int(callback.data.replace("token_off_", ""))
    db.set_wb_token_active(token_id, False)
    await callback.answer("Токен выключен")
    await tokens_menu(callback)


@router.callback_query(F.data.startswith("token_del_"))
async def token_delete(callback: CallbackQuery):
    token_id = int(callback.data.replace("token_del_", ""))
    db.remove_wb_token(token_id)
    await callback.answer("Токен удалён")
    await tokens_menu(callback)


# --- Users ---

@router.callback_query(F.data == "users_menu")
async def users_menu(callback: CallbackQuery):
    users = db.get_allowed_users()
    text = "👥 <b>Пользователи</b>\n\n"

    buttons = []
    for u in users:
        role = "👑" if u["is_owner"] else "👤"
        # Обновить username из Telegram
        try:
            chat = await bot.get_chat(u["telegram_id"])
            username = chat.username or ""
            if username:
                db.update_user_username(u["telegram_id"], username)
        except Exception:
            username = u.get("username") or ""
        if username:
            text += f"{role} <code>{u['telegram_id']}</code> — <a href=\"https://t.me/{username}\">@{username}</a>\n"
        else:
            text += f"{role} <code>{u['telegram_id']}</code>\n"
        display = f"@{username}" if username else str(u["telegram_id"])
        if not u["is_owner"]:
            buttons.append([InlineKeyboardButton(
                text=f"🗑 {display}",
                callback_data=f"user_del_{u['telegram_id']}",
            )])

    buttons.append([InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="user_add")])

    await callback.message.edit_text(text, parse_mode="HTML",
                                      reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                                      disable_web_page_preview=True)
    await callback.answer()


@router.callback_query(F.data == "user_add")
async def user_add_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddUser.waiting_id)
    await callback.message.answer("Введи Telegram ID пользователя (число):")
    await callback.answer()


@router.message(AddUser.waiting_id, ~F.text.in_(MENU_TEXTS))
async def user_add_process(message: Message, state: FSMContext):
    try:
        tid = int(message.text.strip())
    except ValueError:
        await message.answer("Введи число — Telegram ID.")
        return

    username = ""
    try:
        chat = await bot.get_chat(tid)
        username = chat.username or ""
    except Exception:
        pass
    result = db.add_user(tid, username)
    if result:
        display = f"@{username}" if username else str(tid)
        await message.answer(f"✅ Пользователь {display} (<code>{tid}</code>) добавлен.", parse_mode="HTML", reply_markup=main_kb())
    else:
        await message.answer(f"Пользователь уже добавлен.", reply_markup=main_kb())
    await state.clear()


@router.callback_query(F.data.startswith("user_del_"))
async def user_delete_confirm(callback: CallbackQuery):
    tid = int(callback.data.replace("user_del_", ""))
    await callback.answer()
    username = ""
    try:
        chat = await bot.get_chat(tid)
        username = chat.username or ""
    except Exception:
        pass
    display = f"@{username}" if username else str(tid)
    await callback.message.edit_text(
        f"⚠️ Удалить пользователя {display} (<code>{tid}</code>)?\n\nЭто действие нельзя отменить.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"user_confirm_del_{tid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="users_menu")],
        ]),
    )


@router.callback_query(F.data.startswith("user_confirm_del_"))
async def user_delete(callback: CallbackQuery):
    tid = int(callback.data.replace("user_confirm_del_", ""))
    removed = db.remove_user(tid)
    if removed:
        await callback.answer("Удалён")
    else:
        await callback.answer("Нельзя удалить владельца")
    await users_menu(callback)


# --- Alerts ---

ALERT_NAMES = {
    "position_drop_below": "⚠️ Позиция ниже порога",
    "disappeared": "🔴 Пропал из выдачи",
    "position_change": "📉 Резкое изменение позиции",
}

ALERT_SHORT = {
    "position_drop_below": "Ниже порога",
    "disappeared": "Пропал",
    "position_change": "Изменение",
}


@router.message(F.text == "🔔 Уведомления")
async def show_alerts(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    try:
        alert_list = db.get_alerts(uid)
        text = "🔔 <b>Уведомления</b>\n\n"

        buttons = []
        for a in alert_list:
            name = ALERT_NAMES.get(a["alert_type"], a["alert_type"])
            status = "✅" if a["enabled"] else "❌"
            threshold_text = f" (порог: {a['threshold']})" if a["threshold"] else ""
            text += f"{status} {name}{threshold_text}\n"

            toggle_text = "Выкл" if a["enabled"] else "Вкл"
            buttons.append([
                InlineKeyboardButton(
                    text=f"{toggle_text} {name}",
                    callback_data=f"alert_toggle_{a['alert_type']}",
                ),
                InlineKeyboardButton(
                    text="Порог",
                    callback_data=f"alert_thresh_{a['alert_type']}",
                ),
            ])

        await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"show_alerts error: {e}")
        await message.answer("⚠️ Ошибка. Попробуй ещё раз.", reply_markup=main_kb())


@router.callback_query(F.data.startswith("alert_toggle_"))
async def toggle_alert(callback: CallbackQuery):
    uid = callback.from_user.id
    alert_type = callback.data.replace("alert_toggle_", "")
    current = db.get_alerts(uid)
    for a in current:
        if a["alert_type"] == alert_type:
            db.toggle_alert(uid, alert_type, not a["enabled"])
            break
    await callback.answer("Изменено")
    text, buttons = _build_alerts_view(uid)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("alert_thresh_"))
async def set_threshold_start(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    alert_type = callback.data.replace("alert_thresh_", "")
    await state.set_state(SetAlertThreshold.waiting_value)
    await state.update_data(alert_type=alert_type)
    name = ALERT_NAMES.get(alert_type, alert_type)
    await callback.message.answer(f"Введи порог для \"{name}\" (число):")
    await callback.answer()


# --- Geo Scanner ---

@router.message(F.text == "🌍 Гео-сканер")
async def show_geo_scanner(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    arts = db.get_articles(uid)
    if not arts:
        await message.answer("Нет артикулов. Добавь в ⚙️ Настройки → Артикулы.", reply_markup=main_kb())
        return

    regions = config.GEO_REGIONS
    legend = "🌍 <b>Гео-сканер</b>\n\n<b>Регионы сканирования:</b>\n"
    for r in regions:
        legend += f"<b>{r['short']}</b> — {r['name']}\n"
    legend += "\n<i>ПВЗ для сканирования находится в центре города</i>\n\nВыбери артикул:"

    buttons = []
    for a in arts:
        name = a.get("name") or ""
        label = f"{a['sku']} — {name}" if name else a['sku']
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"geo_{a['id']}")])

    await message.answer(legend, parse_mode="HTML",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("geo_"))
async def geo_scan_start(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("geo_", ""))
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return
    queries = db.get_queries(uid, art_id)
    if not queries:
        await callback.answer("Нет запросов у этого артикула")
        return

    await callback.answer()
    sku = article["sku"]
    art_name = article.get("name") or ""
    header = f"{sku} — {art_name}" if art_name else sku
    regions = config.GEO_REGIONS

    msg = await callback.message.edit_text(
        f"⏳ Сканирую <b>{header}</b> по {len(regions)} регионам...\n"
        f"Запросов: {len(queries)} | Это займёт ~{len(queries) * 2} сек.",
        parse_mode="HTML",
    )

    # Scan all queries
    all_results = {}  # {query_text: [{short, position}, ...]}
    for q in queries:
        scan = await parser.geo_scan(sku, q["query"], regions)
        all_results[q["query"]] = scan

    # Build horizontal table
    shorts = [r["short"] for r in regions]
    # Header row
    col_w = 4  # width per city column
    query_w = 28  # width for query column
    header_row = " " * query_w + "".join(s.center(col_w) for s in shorts)
    lines = [header_row]

    for query_text, scan_results in all_results.items():
        q_label = query_text[:query_w - 1]
        padding = " " * max(1, query_w - len(q_label))
        cells = []
        for sr in scan_results:
            pos = sr["position"]
            cells.append(str(pos).center(col_w) if pos else "—".center(col_w))
        lines.append(f"{q_label}{padding}{''.join(cells)}")

    table = "\n".join(lines)
    text = f"🌍 <b>{header}</b>\n\n<pre>{table}</pre>"

    # Split if too long
    if len(text) <= 4096:
        await msg.edit_text(text, parse_mode="HTML")
    else:
        # Split by queries
        half = len(queries) // 2
        items = list(all_results.items())

        for part_idx, part_items in enumerate([items[:half], items[half:]]):
            part_lines = [header_row]
            for query_text, scan_results in part_items:
                q_label = query_text[:query_w - 1]
                padding = " " * max(1, query_w - len(q_label))
                cells = []
                for sr in scan_results:
                    pos = sr["position"]
                    cells.append(str(pos).center(col_w) if pos else "—".center(col_w))
                part_lines.append(f"{q_label}{padding}{''.join(cells)}")

            part_table = "\n".join(part_lines)
            part_text = f"🌍 <b>{header}</b> ({part_idx + 1}/2)\n\n<pre>{part_table}</pre>"
            if part_idx == 0:
                await msg.edit_text(part_text, parse_mode="HTML")
            else:
                await bot.send_message(callback.from_user.id, part_text, parse_mode="HTML")


# --- Shelf (Recommendation shelves) ---

# -- Main menu "Полки" button: select article → instant scan --

@router.message(F.text == "Полки")
async def shelf_menu(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    arts = db.get_articles(uid)
    if not arts:
        await message.answer("Нет артикулов. Добавь в ⚙️ Настройки.", reply_markup=main_kb())
        return

    buttons = []
    for a in arts:
        comp_count = db.count_competitors(uid, a["id"])
        name = a.get("name") or ""
        label = f"{a['sku']}"
        if name:
            label += f" — {name}"
        label += f" ({comp_count} конк.)"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"shelf_check_{a['id']}")])
    buttons.append([InlineKeyboardButton(text="▶️ Проверить ВСЕ", callback_data="shelf_check_all")])

    await message.answer(
        "🏪 <b>Полки</b>\n\nВыбери артикул для проверки:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


# -- Settings "Полки": shelf_{id} shows competitor management --

def _shelf_article_kb(article_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить конкурента", callback_data=f"shelf_add_{article_id}")],
        [InlineKeyboardButton(text="🗑 Удалить конкурента", callback_data=f"shelf_dellist_{article_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings_shelves")],
    ])


@router.callback_query(F.data.startswith("shelf_") & ~F.data.startswith("shelf_check_")
                        & ~F.data.startswith("shelf_add_") & ~F.data.startswith("shelf_dellist_")
                        & ~F.data.startswith("shelf_rm_"))
async def shelf_article_settings(callback: CallbackQuery):
    """Settings → Shelves → article: manage competitors."""
    uid = callback.from_user.id
    art_id = int(callback.data.replace("shelf_", ""))
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return
    await callback.answer()

    sku = article["sku"]
    name = article.get("name") or ""
    competitors = db.get_competitors(uid, art_id)

    text = f"🏪 <b>Полки: {escape(sku)}</b>"
    if name:
        text += f" — {escape(name)}"
    text += f"\n\nКонкуренты ({len(competitors)}/{db.MAX_COMPETITORS_PER_ARTICLE}):"

    if competitors:
        for i, c in enumerate(competitors):
            cname = f" — {escape(c['competitor_name'])}" if c.get("competitor_name") else ""
            text += f"\n{i+1}. <b>{c['competitor_sku']}</b>{cname}"
    else:
        text += "\nПусто. Добавь конкурентов."

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_shelf_article_kb(art_id))


@router.callback_query(F.data.startswith("shelf_add_"))
async def shelf_add_start(callback: CallbackQuery, state: FSMContext):
    art_id = int(callback.data.replace("shelf_add_", ""))
    uid = callback.from_user.id
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return

    comp_count = db.count_competitors(uid, art_id)
    if comp_count >= db.MAX_COMPETITORS_PER_ARTICLE:
        await callback.answer(f"Максимум {db.MAX_COMPETITORS_PER_ARTICLE} конкурентов", show_alert=True)
        return

    await state.set_state(AddCompetitor.waiting_sku)
    await state.update_data(art_id=art_id, our_sku=article["sku"])
    await callback.message.answer(
        f"Введи артикулы конкурентов для <b>{escape(article['sku'])}</b>.\n"
        f"Каждый артикул с новой строки (макс. {db.MAX_COMPETITORS_PER_ARTICLE - comp_count}):",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(AddCompetitor.waiting_sku, ~F.text.in_(MENU_TEXTS))
async def shelf_add_process(message: Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    art_id = data.get("art_id")
    our_sku = data.get("our_sku")
    article = db.get_article_by_id(uid, art_id) if art_id else None
    if not article:
        await message.answer("Артикул не найден.")
        await state.clear()
        return

    lines = [line.strip() for line in message.text.split("\n") if line.strip()]
    added = []
    skipped = []
    for line in lines:
        sku = line.strip()
        if not sku.isdigit() or len(sku) < 5:
            skipped.append(f"{sku} (не число)")
            continue
        if sku == our_sku:
            skipped.append(f"{sku} (свой артикул)")
            continue
        # Fetch brand from WB
        brand = await parser.fetch_brand(sku)
        result = db.add_competitor(uid, article["id"], sku, name=brand)
        if result:
            added.append(f"{sku} — {brand}" if brand else sku)
        else:
            skipped.append(f"{sku} (дубликат/лимит)")

    text = f"✅ Добавлено: {len(added)}"
    if added:
        text += "\n" + "\n".join(added)
    if skipped:
        text += f"\n⏭ Пропущено: {', '.join(skipped)}"

    await message.answer(text, parse_mode="HTML", reply_markup=_shelf_article_kb(article["id"]))
    await state.clear()


@router.callback_query(F.data.startswith("shelf_dellist_"))

async def shelf_dellist(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("shelf_dellist_", ""))
    competitors = db.get_competitors(uid, art_id)
    if not competitors:
        await callback.answer("Нет конкурентов для удаления")
        return
    await callback.answer()

    buttons = []
    for c in competitors:
        label = c["competitor_sku"]
        if c.get("competitor_name"):
            label += f" — {c['competitor_name']}"
        buttons.append([InlineKeyboardButton(
            text=f"🗑 {label}",
            callback_data=f"shelf_rm_{c['id']}_{art_id}",
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"shelf_{art_id}")])

    await callback.message.edit_text(
        "Выбери конкурента для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("shelf_rm_"))
async def shelf_remove(callback: CallbackQuery):
    uid = callback.from_user.id
    parts = callback.data.split("_")
    comp_id = int(parts[2])
    art_id = int(parts[3])
    db.remove_competitor(uid, comp_id)
    await callback.answer("Удалён")

    # Refresh article view
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.message.edit_text("Артикул не найден.")
        return

    sku = article["sku"]
    name = article.get("name") or ""
    competitors = db.get_competitors(uid, art_id)

    text = f"🏪 <b>Полки: {escape(sku)}</b>"
    if name:
        text += f" — {escape(name)}"
    text += f"\n\nКонкуренты ({len(competitors)}/{db.MAX_COMPETITORS_PER_ARTICLE}):"
    if competitors:
        for i, c in enumerate(competitors):
            cname = f" — {escape(c['competitor_name'])}" if c.get("competitor_name") else ""
            text += f"\n{i+1}. <b>{c['competitor_sku']}</b>{cname}"
    else:
        text += "\nПусто."

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_shelf_article_kb(art_id))


@router.callback_query(F.data == "shelf_check_all")
async def shelf_check_all(callback: CallbackQuery):
    uid = callback.from_user.id
    arts = db.get_articles(uid)
    # Filter articles that have competitors
    arts_with_comps = []
    for a in arts:
        comps = db.get_competitors(uid, a["id"])
        if comps:
            arts_with_comps.append((a, comps))
    if not arts_with_comps:
        await callback.answer("Нет артикулов с конкурентами. Добавь в ⚙️ Настройки → Полки.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_text(
        f"⏳ Сканирую полки для {len(arts_with_comps)} артикулов...",
        parse_mode="HTML",
    )

    chat_id = callback.message.chat.id
    msg_id = callback.message.message_id
    task = asyncio.create_task(_do_shelf_check_all(uid, chat_id, msg_id, arts_with_comps))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@router.callback_query(F.data.startswith("shelf_check_"))
async def shelf_check(callback: CallbackQuery):
    uid = callback.from_user.id
    art_id = int(callback.data.replace("shelf_check_", ""))
    article = db.get_article_by_id(uid, art_id)
    if not article:
        await callback.answer("Артикул не найден")
        return

    competitors = db.get_competitors(uid, art_id)
    if not competitors:
        await callback.answer("Добавь конкурентов в ⚙️ Настройки → Полки", show_alert=True)
        return

    await callback.answer()
    sku = article["sku"]
    art_name = article.get("name") or ""

    await callback.message.edit_text(
        f"⏳ Сканирую полки для <b>{sku}</b> ({len(competitors)} конкурентов)...",
        parse_mode="HTML",
    )

    chat_id = callback.message.chat.id
    msg_id = callback.message.message_id
    task = asyncio.create_task(_do_shelf_check(uid, chat_id, msg_id, article, competitors))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _do_shelf_check(uid: int, chat_id: int, msg_id: int, article: dict, competitors: list):
    """Background task: scan recommendation shelves."""
    try:
        sku = article["sku"]
        art_name = article.get("name") or ""

        # Fill missing brand names
        for c in competitors:
            if not c.get("competitor_name"):
                brand = await parser.fetch_brand(c["competitor_sku"])
                if brand:
                    c["competitor_name"] = brand
                    db.update_competitor_name(uid, c["id"], brand)

        comp_skus = [c["competitor_sku"] for c in competitors]

        start = time.time()
        results = await parser.recom_scan_all(sku, comp_skus)
        elapsed = time.time() - start

        text = _format_shelf_results(sku, competitors, results, elapsed, name=art_name)
        await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Shelf check error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


async def _do_shelf_check_all(uid: int, chat_id: int, msg_id: int, arts_with_comps: list):
    """Background task: scan shelves for all articles."""
    try:
        start = time.time()
        all_texts = []

        for article, competitors in arts_with_comps:
            sku = article["sku"]
            art_name = article.get("name") or ""

            # Fill missing brand names
            for c in competitors:
                if not c.get("competitor_name"):
                    brand = await parser.fetch_brand(c["competitor_sku"])
                    if brand:
                        c["competitor_name"] = brand
                        db.update_competitor_name(uid, c["id"], brand)

            comp_skus = [c["competitor_sku"] for c in competitors]
            results = await parser.recom_scan_all(sku, comp_skus)
            block = _format_shelf_block(sku, competitors, results, name=art_name)
            all_texts.append(block)

        elapsed = time.time() - start
        now = datetime.now().strftime("%H:%M %d.%m")
        full_text = f"{now}{_elapsed_str(elapsed)}\n\n" + "\n\n".join(all_texts)

        if len(full_text) <= 4000:
            await bot.edit_message_text(full_text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
        else:
            await bot.edit_message_text(f"{now}{_elapsed_str(elapsed)}", chat_id=chat_id, message_id=msg_id)
            for block in all_texts:
                await bot.send_message(chat_id, block, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Shelf check all error: {e}")
        try:
            await bot.edit_message_text(f"Ошибка: {e}", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass


def _format_shelf_block(sku: str, competitors: list, results: dict, name: str = "") -> str:
    """Format shelf results as a <pre> block (no timestamp header)."""
    header = f"{sku} — {name}" if name else sku
    query_lines = []
    for c in competitors:
        comp_sku = c["competitor_sku"]
        comp_name = c.get("competitor_name") or ""
        r = results.get(comp_sku, {})
        if r.get("error"):
            pos_str = "ERR"
        elif r.get("position") is not None:
            pos_str = str(r["position"])
        else:
            pos_str = "—"
        label = comp_sku
        if comp_name:
            label += f" {comp_name}"
        if len(label) > 26:
            label = label[:25] + "…"
        padding = max(1, 28 - len(label))
        query_lines.append(f"{label}{' ' * padding}│ {pos_str:>4} │")
    col_header = f"{'':28}│  поз │"
    col_sep = f"{'':28}│------│"
    lines = [header, col_header, col_sep] + query_lines
    return f"<pre>{chr(10).join(lines)}</pre>"


def _format_shelf_results(sku: str, competitors: list, results: dict,
                          elapsed: float = None, name: str = "") -> str:
    """Format shelf scan results."""
    now = datetime.now().strftime("%H:%M %d.%m")
    header = f"{sku} — {name}" if name else sku

    query_lines = []
    errors = 0
    for c in competitors:
        comp_sku = c["competitor_sku"]
        comp_name = c.get("competitor_name") or ""
        r = results.get(comp_sku, {})

        if r.get("error"):
            pos_str = "ERR"
            errors += 1
        elif r.get("position") is not None:
            pos_str = str(r["position"])
        else:
            pos_str = "—"

        label = comp_sku
        if comp_name:
            label += f" {comp_name}"
        if len(label) > 26:
            label = label[:25] + "…"
        padding = max(1, 28 - len(label))
        query_lines.append(f"{label}{' ' * padding}│ {pos_str:>4} │")

    col_header = f"{'':28}│  поз │"
    col_sep = f"{'':28}│------│"
    lines = [header, col_header, col_sep] + query_lines

    text = f"<b>🏪 {escape(sku)}</b> | {now}{_elapsed_str(elapsed)}\n\n"
    text += f"<pre>{chr(10).join(lines)}</pre>"

    if errors:
        text += f"\n⚠️ {errors} запросов не выполнены"
    return text


# --- Scheduler ---

async def scheduled_parse():
    """Auto-parse for all users with auto_check articles using Chrome positions."""
    logger.info("Scheduled auto-parse started")
    users = db.get_allowed_users()

    async def parse_user(uid):
        try:
            all_arts = db.get_articles(uid)
            arts = [a for a in all_arts if a.get("auto_check")]
            if not arts:
                return

            all_blocks = []
            start = time.time()

            # Submit all to queue
            task_infos = []
            for article in arts:
                sku = article["sku"]
                nm_id = int(sku) if sku.isdigit() else 0
                if not nm_id:
                    continue
                queries_list = db.get_queries(uid, article["id"])
                if not queries_list:
                    continue
                keywords = [q["query"] for q in queries_list]
                future = await position_queue.submit(uid, nm_id, keywords, label=sku)
                task_infos.append((article, queries_list, keywords, future))

            for article, queries_list, keywords, future in task_infos:
                sku = article["sku"]
                art_name = article.get("name") or ""
                positions = await future
                lines = _format_evirma_block(sku, keywords, positions, name=art_name)
                all_blocks.append(lines)

                # Save positions for charts
                for q in queries_list:
                    pos_data = positions.get(q["query"], {})
                    promo_pos = pos_data.get("promo_pos")
                    db.save_result(uid, article["id"], q["id"], promo_pos, 1 if promo_pos and promo_pos <= 300 else 2)

            elapsed = time.time() - start

            if not all_blocks:
                return

            now = datetime.now().strftime("%H:%M %d.%m")
            separator = ["────────────────────────────┴──────┴──────┘"]
            all_lines = []
            for i, block in enumerate(all_blocks):
                all_lines.extend(block)
                if i < len(all_blocks) - 1:
                    all_lines.extend(separator); all_lines.append("")

            text = f"{now} | {elapsed:.1f}с\n\n<pre>{chr(10).join(all_lines)}</pre>"

            if len(text) > 4000:
                for i, block in enumerate(all_blocks):
                    block_text = f"<pre>{chr(10).join(block)}</pre>"
                    await bot.send_message(uid, block_text, parse_mode="HTML")
            else:
                await bot.send_message(uid, text, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Scheduled parse error for {uid}: {e}")

    for u in users:
        await parse_user(u["telegram_id"])


def reschedule_parser():
    """Update scheduler interval."""
    # Use owner's interval as global
    users = db.get_allowed_users()
    owner = next((u for u in users if u["is_owner"]), None)
    uid = owner["telegram_id"] if owner else 0
    interval = int(db.get_setting(uid, "interval_minutes") or 15) if uid else 15

    if scheduler.get_job("auto_parse"):
        scheduler.remove_job("auto_parse")

    scheduler.add_job(
        scheduled_parse,
        trigger=IntervalTrigger(minutes=interval),
        id="auto_parse",
        replace_existing=True,
    )
    logger.info(f"Scheduler set to {interval} min interval")


# --- Main ---

async def main():
    reschedule_parser()
    scheduler.start()
    await position_queue.start()

    logger.info("Bot starting...")
    logger.info("Start polling")
    await dp.start_polling(bot, polling_timeout=1)


if __name__ == "__main__":
    db.init_db()
    # Get x_wbaas_token BEFORE starting asyncio loop
    if config.PARSE_MODE == "proxy":
        import proxy_positions
        proxy_positions.refresh_wbaas_tokens()
    else:
        parser.refresh_wbaas_token_sync()
    asyncio.run(main())
