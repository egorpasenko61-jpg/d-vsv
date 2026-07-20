import asyncio

import json

import os

import random

import time

from datetime import datetime

from aiogram import Bot, Dispatcher, F

from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from aiogram.filters import Command

from aiogram.fsm.context import FSMContext

from aiogram.fsm.state import State, StatesGroup

from aiogram.fsm.storage.memory import MemoryStorage

from telethon import TelegramClient

from telethon.errors import FloodWaitError, SessionPasswordNeededError, ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError

from telethon.tl.types import User, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth, UserStatusOnline, UserStatusOffline, PeerUser

from telethon.tl.functions.messages import SearchGlobalRequest

import aiohttp

import aiosqlite


# Глобальные настройки API

API_ID = 32155028

API_HASH = "ec906474420c7cc518e2245d5829924a"

BOT_TOKEN = "7860968550:AAHNx_mJHsDrohp0DV60eTy1wCdl8gKxqmE"


# API Токен от @CryptoBot (Crypto Pay)

CRYPTO_BOT_TOKEN = "611722:AARQbBBi1uLtIjPPcr9fwNl24y0SVbroSZG" 


# Настройки SaaS

ADMIN_ID = 7521801228  # Сюда будут приходить уведомления о покупках

FREE_LIMIT = 50

SUB_PRICE_USD = 1.5

WATERMARK = "\n\nОтправлено с помощью @nonewin_bot"

DB_PATH = "bot_database.db"


config_lock = asyncio.Lock()


def get_user_config_path(user_id: int) -> str:

    return f"config_{user_id}.json"


def get_default_config() -> dict:

    return {

        "delays": {"min": 10, "max": 30},

        "stats": {"sent_count": 0},

        "scenarios": {},

        "chats": [],

        "status": "stopped",

        "session_names": [],

        "is_premium": False,

        "daily_sent": 0,

        "last_sent_date": ""

    }


telethon_accounts = {}

active_auths = {}


class BotStates(StatesGroup):

    waiting_for_phone = State()

    waiting_for_code = State()

    waiting_for_password = State()

    waiting_for_chats = State()

    waiting_for_scenario_text = State()

    waiting_for_delay_min = State()

    waiting_for_delay_max = State()

    waiting_for_parse_groups = State()

    waiting_for_parse_account = State()

    waiting_for_db_mailing_text = State()

    waiting_for_search_keyword = State()

    waiting_for_search_groups = State()


async def load_config(user_id: int) -> dict:

    file_path = get_user_config_path(user_id)

    async with config_lock:

        if not os.path.exists(file_path):

            with open(file_path, "w", encoding="utf-8") as f:

                json.dump(get_default_config(), f, ensure_ascii=False, indent=4)

        with open(file_path, "r", encoding="utf-8") as f:

            data = json.load(f)

            

            # Проверка и сброс дневного лимита при наступлении нового дня

            today = datetime.now().strftime("%Y-%m-%d")

            if data.get("last_sent_date") != today:

                data["daily_sent"] = 0

                data["last_sent_date"] = today

            return data


async def save_config(user_id: int, config_data: dict):

    file_path = get_user_config_path(user_id)

    async with config_lock:

        with open(file_path, "w", encoding="utf-8") as f:

            json.dump(config_data, f, ensure_ascii=False, indent=4)


# ============================================================

#   БАЗА ДАННЫХ: парсер юзеров + рассылка по базе + архив

# ============================================================


async def init_db():

    """Создаёт таблицы если их ещё нет. Вызывать при старте."""

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""

            CREATE TABLE IF NOT EXISTS parsed_users (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                owner_id INTEGER NOT NULL,

                user_id INTEGER NOT NULL,

                username TEXT,

                first_name TEXT,

                last_name TEXT,

                source_group TEXT,

                is_bot INTEGER DEFAULT 0,

                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                archived INTEGER DEFAULT 0,

                archived_at TIMESTAMP,

                UNIQUE(owner_id, user_id)

            )

        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_owner_archived ON parsed_users(owner_id, archived)")

        await db.execute("""CREATE TABLE IF NOT EXISTS db_mailing_state (

            owner_id INTEGER PRIMARY KEY,

            status TEXT DEFAULT 'stopped',

            text TEXT,

            account_name TEXT,

            total INTEGER DEFAULT 0,

            sent INTEGER DEFAULT 0,

            failed INTEGER DEFAULT 0,

            current_index INTEGER DEFAULT 0,

            include_archived INTEGER DEFAULT 0

        )""")

        await db.commit()


async def add_parsed_users_bulk(owner_id: int, users: list, source_group: str) -> int:

    """Добавляет юзеров пачкой. Возвращает количество реально добавленных (без дублей)."""

    if not users:

        return 0

    added = 0

    async with aiosqlite.connect(DB_PATH) as db:

        for u in users:

            try:

                await db.execute(

                    """INSERT OR IGNORE INTO parsed_users

                       (owner_id, user_id, username, first_name, last_name, source_group, is_bot)

                       VALUES (?, ?, ?, ?, ?, ?, ?)""",

                    (owner_id, u["user_id"], u.get("username"), u.get("first_name"),

                     u.get("last_name"), source_group, 1 if u.get("is_bot") else 0)

                )

                added += 1

            except Exception as e:

                print(f"[DB] Ошибка вставки юзера {u.get('user_id')}: {e}")

        await db.commit()

    return added


async def get_user_db_stats(owner_id: int) -> dict:

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(

            "SELECT COUNT(*), SUM(archived), SUM(CASE WHEN is_bot=1 THEN 1 ELSE 0 END) FROM parsed_users WHERE owner_id=?",

            (owner_id,)

        )

        row = await cur.fetchone()

        total, archived, bots = row[0] or 0, row[1] or 0, row[2] or 0

        cur = await cur.execute(

            "SELECT COUNT(DISTINCT source_group) FROM parsed_users WHERE owner_id=?",

            (owner_id,)

        )

        groups_count = (await cur.fetchone())[0] or 0

    return {

        "total": total,

        "active": total - archived,

        "archived": archived,

        "bots": bots,

        "groups": groups_count,

    }


async def get_active_user_ids(owner_id: int, include_archived: bool = False, limit: int = 50000):

    """Возвращает список (db_id, user_id, username, first_name) для рассылки."""

    sql = "SELECT id, user_id, username, first_name FROM parsed_users WHERE owner_id=?"

    if not include_archived:

        sql += " AND archived=0"

    sql += " ORDER BY id LIMIT ?"

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(sql, (owner_id, limit))

        rows = await cur.fetchall()

    return rows


async def mark_user_archived(db_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(

            "UPDATE parsed_users SET archived=1, archived_at=CURRENT_TIMESTAMP WHERE id=?",

            (db_id,)

        )

        await db.commit()


async def unarchive_all_for_owner(owner_id: int) -> int:

    """Снимает флаг архива со всех юзеров владельца."""

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(

            "UPDATE parsed_users SET archived=0, archived_at=NULL WHERE owner_id=?",

            (owner_id,)

        )

        await db.commit()

        return cur.rowcount or 0


async def clear_all_for_owner(owner_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("DELETE FROM parsed_users WHERE owner_id=?", (owner_id,))

        await db.execute("DELETE FROM db_mailing_state WHERE owner_id=?", (owner_id,))

        await db.commit()


async def get_db_mailing_state(owner_id: int) -> dict:

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute("SELECT * FROM db_mailing_state WHERE owner_id=?", (owner_id,))

        row = await cur.fetchone()

        if not row:

            return {"status": "stopped", "text": None, "account_name": None,

                    "total": 0, "sent": 0, "failed": 0, "current_index": 0, "include_archived": 0}

        cols = [d[0] for d in cur.description]

        return dict(zip(cols, row))


async def save_db_mailing_state(owner_id: int, **kwargs):

    current = await get_db_mailing_state(owner_id)

    current.update(kwargs)

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(

            """INSERT INTO db_mailing_state

               (owner_id, status, text, account_name, total, sent, failed, current_index, include_archived)

               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

               ON CONFLICT(owner_id) DO UPDATE SET

                 status=excluded.status, text=excluded.text, account_name=excluded.account_name,

                 total=excluded.total, sent=excluded.sent, failed=excluded.failed,

                 current_index=excluded.current_index, include_archived=excluded.include_archived""",

            (owner_id, current["status"], current.get("text"), current.get("account_name"),

             current.get("total", 0), current.get("sent", 0), current.get("failed", 0),

             current.get("current_index", 0), current.get("include_archived", 0))

        )

        await db.commit()


# Взаимодействие с CryptoBot API (Официальный URL: https://pay.crypt.bot/api/)

async def create_crypto_invoice(amount: float, user_id: int):

    url = "https://pay.crypt.bot/api/createInvoice"

    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}

    payload = {

        "asset": "USDT",

        "amount": str(amount),

        "description": "Подписка на премиум-план EgorMailer",

        "payload": str(user_id),

        "paid_btn_name": "openBot",

        "paid_btn_url": "https://t.me/nonewin_bot"

    }

    try:

        async with aiohttp.ClientSession() as session:

            async with session.post(url, json=payload, headers=headers) as resp:

                body = await resp.text()

                if resp.status == 200:

                    res_json = await resp.json() if body else {}

                    if res_json.get("ok"):

                        return res_json["result"]["pay_url"], res_json["result"]["invoice_id"], None

                    else:

                        # API ответил 200, но ok=false — покажем причину

                        err = res_json.get("error", {})

                        msg = f"API ok=false: name={err.get('name')}, code={err.get('code')}"

                        print(f"[CryptoPay] {msg}")

                        return None, None, msg

                else:

                    err_msg = f"HTTP {resp.status}: {body[:300]}"

                    print(f"[CryptoPay] {err_msg}")

                    return None, None, err_msg

    except aiohttp.ClientConnectorError as e:

        err_msg = f"Нет соединения с pay.crypt.bot: {e}"

        print(f"[CryptoPay] {err_msg}")

        return None, None, err_msg

    except Exception as e:

        err_msg = f"Исключение при создании счета: {e}"

        print(f"[CryptoPay] {err_msg}")

        return None, None, err_msg

    return None, None, "Неизвестная ошибка"


async def check_crypto_invoice(invoice_id: int):

    url = f"https://pay.crypt.bot/api/getInvoices?invoice_ids={invoice_id}"

    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}

    try:

        async with aiohttp.ClientSession() as session:

            async with session.get(url, headers=headers) as resp:

                if resp.status == 200:

                    res_json = await resp.json()

                    if res_json.get("ok") and res_json["result"]["items"]:

                        return res_json["result"]["items"][0]["status"] == "paid"

                    else:

                        err = res_json.get("error", {})

                        print(f"[CryptoPay] getInvoices ok=false: {err}")

                else:

                    print(f"[CryptoPay] getInvoices HTTP {resp.status}: {await resp.text()}")

    except Exception as e:

        print(f"[CryptoPay] Ошибка проверки счета: {e}")

    return False


async def init_telethon_accounts_for_user(user_id: int):

    global telethon_accounts

    config = await load_config(user_id)

    if user_id not in telethon_accounts:

        telethon_accounts[user_id] = {}


    for session_name in config.get("session_names", []):

        # Не пересоздаём клиент, если он уже жив

        if session_name in telethon_accounts[user_id]:

            try:

                existing = telethon_accounts[user_id][session_name]

                if existing.is_connected() and await existing.is_user_authorized():

                    continue

            except Exception:

                pass

        client = TelegramClient(session_name, API_ID, API_HASH)

        try:

            await client.connect()

            if await client.is_user_authorized():

                telethon_accounts[user_id][session_name] = client

                print(f"✅ [{user_id}] Сессия {session_name} загружена")

            else:

                print(f"⚠️ [{user_id}] Сессия {session_name} не авторизована — пользователь должен войти заново")

                try:

                    await client.disconnect()

                except Exception:

                    pass

        except Exception as e:

            print(f"❌ [{user_id}] Ошибка загрузки сессии {session_name}: {e}")


async def init_all_existing_accounts():

    for file in os.listdir("."):

        if file.startswith("config_") and file.endswith(".json"):

            try:

                user_id = int(file.split("_")[1].split(".")[0])

                await init_telethon_accounts_for_user(user_id)

            except ValueError:

                continue


# ============================================================

#   ПАРСЕР УЧАСТНИКОВ ГРУПП

# ============================================================


active_parsers = {}  # owner_id -> asyncio.Task


def _is_real_user(user) -> bool:

    """Отфильтровывает ботов, удалённые аккаунты и пустых юзеров."""

    if not isinstance(user, User):

        return False

    if getattr(user, "bot", False):

        return False

    if getattr(user, "deleted", False):

        return False

    if not user.id:

        return False

    return True


def _status_to_str(user) -> str:

    s = getattr(user, "status", None)

    if isinstance(s, UserStatusOnline): return "online"

    if isinstance(s, UserStatusRecently): return "recently"

    if isinstance(s, UserStatusLastWeek): return "last_week"

    if isinstance(s, UserStatusLastMonth): return "last_month"

    if isinstance(s, UserStatusOffline): return "offline"

    return "unknown"


async def parse_users_from_groups(bot: Bot, owner_id: int, groups: list, account_name: str, status_msg: Message):

    """

    Фоновый парсинг. Шлёт прогресс в status_msg, по окончанию — итог.

    """

    client = telethon_accounts.get(owner_id, {}).get(account_name)

    if client is None:

        await status_msg.edit_text(f"❌ Аккаунт **{account_name}** не найден или не подключён. Откройте ‘Аккаунты / Сессии’ и добавьте заново.")

        return


    total_added = 0

    total_seen = 0

    groups_done = 0

    groups_failed = []

    await status_msg.edit_text(f"👥 Парсинг запущен. Групп в очереди: **{len(groups)}**\nАккаунт: **{account_name}**\n\nПрогресс будет обновляться…")


    for group_ref in groups:

        group_label = group_ref

        try:

            try:

                entity = await client.get_entity(group_ref)

            except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):

                groups_failed.append(f"{group_ref} (не найден)")

                continue

            except (ChannelPrivateError, ChatAdminRequiredError) as e:

                groups_failed.append(f"{group_ref} (нет доступа)")

                continue

            except Exception as e:

                groups_failed.append(f"{group_ref} ({type(e).__name__})")

                continue


            group_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(group_ref)

            group_label = group_title


            batch = []

            seen_in_group = 0

            last_progress = time.time()

            try:

                async for u in client.iter_participants(entity, aggressive=True):

                    if not _is_real_user(u):

                        continue

                    seen_in_group += 1

                    total_seen += 1

                    batch.append({

                        "user_id": u.id,

                        "username": u.username,

                        "first_name": u.first_name,

                        "last_name": u.last_name,

                        "is_bot": False,

                    })

                    # Пачками по 200 — пишем в БД и шлём прогресс

                    if len(batch) >= 200:

                        added = await add_parsed_users_bulk(owner_id, batch, group_title)

                        total_added += added

                        batch.clear()

                        if time.time() - last_progress > 3:

                            try:

                                await status_msg.edit_text(

                                    f"👥 Парсинг...\n\n"

                                    f"📌 Сейчас: **{group_title}**\n"

                                    f"👁 Обработано в группе: {seen_in_group}\n"

                                    f"✅ Всего новых в базе: {total_added}\n"

                                    f"📦 Групп готово: {groups_done}/{len(groups)}"

                                )

                            except Exception:

                                pass

                            last_progress = time.time()

                    # Лёгкий троттлинг чтобы не упереться в FloodWait

                    if seen_in_group % 500 == 0:

                        await asyncio.sleep(1)

                if batch:

                    added = await add_parsed_users_bulk(owner_id, batch, group_title)

                    total_added += added

            except FloodWaitError as e:

                # Ждём FloodWait и продолжаем со следующей группой (эту бросаем)

                groups_failed.append(f"{group_title} (FloodWait {e.seconds}s)")

                try:

                    await status_msg.edit_text(

                        f"⏳ FloodWait {e.seconds}s на группе **{group_title}** — пропускаю и иду дальше.\n"

                        f"✅ Уже добавлено: {total_added}\n"

                        f"📦 Осталось групп: {len(groups) - groups_done - 1}"

                    )

                except Exception:

                    pass

                await asyncio.sleep(min(e.seconds, 60))

            except Exception as e:

                groups_failed.append(f"{group_title} ({type(e).__name__}: {e})")


            groups_done += 1

            await asyncio.sleep(2)  # пауза между группами

        except Exception as e:

            groups_failed.append(f"{group_label} ({type(e).__name__})")


    # Итог

    stats = await get_user_db_stats(owner_id)

    fail_text = "\n".join(f"  • {g}" for g in groups_failed) if groups_failed else "  —"

    await status_msg.edit_text(

        f"✅ **Парсинг завершён!**\n\n"

        f"📦 Обработано групп: {groups_done}/{len(groups)}\n"

        f"👁 Всего юзеров просмотрено: {total_seen}\n"

        f"🆕 Добавлено в базу (новых): {total_added}\n\n"

        f"📊 **Состояние базы:**\n"

        f"  • Всего: {stats['total']}\n"

        f"  • Активных (не в архиве): {stats['active']}\n"

        f"  • В архиве: {stats['archived']}\n"

        f"  • Источников-групп: {stats['groups']}\n\n"

        f"⚠️ **Пропущены:**\n{fail_text}",

        reply_markup=get_db_keyboard()

    )

    print(f"[PARSER] [{owner_id}] Готово. +{total_added} юзеров, пропущено групп: {len(groups_failed)}")


# ============================================================

#   РАССЫЛКА ПО БАЗЕ (ЛС каждому юзеру → автоархив)

# ============================================================


active_db_mailings = {}  # owner_id -> asyncio.Task


# ============================================================

#   ПОИСК ЮЗЕРОВ ПО КЛЮЧЕВОМУ СЛОВУ (через сообщения)

# ============================================================


active_searches = {}  # owner_id -> asyncio.Task


def _extract_user_from_msg(msg) -> dict | None:

    """Извлекает user_id (+ опционально username/first_name) из сообщения."""

    if not msg or not msg.from_id:

        return None

    if not isinstance(msg.from_id, PeerUser):

        return None

    user_id = msg.from_id.user_id

    if not user_id:

        return None

    sender = getattr(msg, "sender", None)

    return {

        "user_id": user_id,

        "username": getattr(sender, "username", None) if sender else None,

        "first_name": getattr(sender, "first_name", None) if sender else None,

        "last_name": getattr(sender, "last_name", None) if sender else None,

    }


async def search_in_groups(bot: Bot, owner_id: int, keyword: str, groups: list, account_name: str, status_msg: Message):

    """

    Поиск сообщений с keyword по списку чатов.

    Каждое найденное сообщение → user_id автора → в БД.

    """

    client = telethon_accounts.get(owner_id, {}).get(account_name)

    if client is None:

        await status_msg.edit_text(f"❌ Аккаунт **{account_name}** не подключён.")

        return


    await status_msg.edit_text(

        f"🔍 **Поиск запущен**\n\n"

        f"🔑 Ключевое слово: `{keyword}`\n"

        f"👤 Аккаунт: **{account_name}**\n"

        f"📦 Чатов в очереди: {len(groups)}\n\n"

        f"⏳ Прогресс будет обновляться…"

    )


    total_found = 0

    total_added = 0

    total_messages = 0

    groups_done = 0

    groups_failed = []


    for group_ref in groups:

        group_label = group_ref

        try:

            try:

                entity = await client.get_entity(group_ref)

            except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):

                groups_failed.append(f"{group_ref} (не найден)")

                continue

            except (ChannelPrivateError, ChatAdminRequiredError):

                groups_failed.append(f"{group_ref} (нет доступа)")

                continue

            except Exception as e:

                groups_failed.append(f"{group_ref} ({type(e).__name__})")

                continue


            group_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(group_ref)

            group_label = group_title


            batch = []

            last_progress = time.time()

            try:

                # limit=300 на чат — больше Telegram обычно не отдаёт по поиску

                async for msg in client.iter_messages(entity, search=keyword, limit=300):

                    total_messages += 1

                    u = _extract_user_from_msg(msg)

                    if u is None:

                        continue

                    total_found += 1

                    batch.append(u)

                    if len(batch) >= 100:

                        added = await add_parsed_users_bulk(owner_id, batch, f"search:{keyword}:{group_title}")

                        total_added += added

                        batch.clear()

                    if total_messages % 50 == 0 and time.time() - last_progress > 3:

                        try:

                            await status_msg.edit_text(

                                f"🔍 **Поиск…**\n\n"

                                f"📌 Сейчас: **{group_title}**\n"

                                f"🔎 Просмотрено сообщений: {total_messages}\n"

                                f"👤 Найдено авторов: {total_found}\n"

                                f"✅ Добавлено в базу: {total_added}\n"

                                f"📦 Групп готово: {groups_done}/{len(groups)}"

                            )

                        except Exception:

                            pass

                        last_progress = time.time()

                if batch:

                    added = await add_parsed_users_bulk(owner_id, batch, f"search:{keyword}:{group_title}")

                    total_added += added

            except FloodWaitError as e:

                groups_failed.append(f"{group_title} (FloodWait {e.seconds}s)")

                try:

                    await status_msg.edit_text(

                        f"⏳ FloodWait {e.seconds}s на **{group_title}** — пропускаю.\n"

                        f"✅ Уже найдено: {total_found} авторов, добавлено: {total_added}"

                    )

                except Exception:

                    pass

                await asyncio.sleep(min(e.seconds, 60))

            except Exception as e:

                groups_failed.append(f"{group_title} ({type(e).__name__}: {e})")


            groups_done += 1

            await asyncio.sleep(2)

        except Exception as e:

            groups_failed.append(f"{group_label} ({type(e).__name__})")


    stats = await get_user_db_stats(owner_id)

    fail_text = "\n".join(f"  • {g}" for g in groups_failed) if groups_failed else "  —"

    await status_msg.edit_text(

        f"✅ **Поиск завершён!**\n\n"

        f"🔑 Ключевое слово: `{keyword}`\n"

        f"📦 Обработано чатов: {groups_done}/{len(groups)}\n"

        f"🔎 Просмотрено сообщений: {total_messages}\n"

        f"👤 Уникальных авторов найдено: {total_found}\n"

        f"🆕 Добавлено в базу (новых): {total_added}\n\n"

        f"📊 **Состояние базы:**\n"

        f"  • Всего: {stats['total']}\n"

        f"  • Активных: {stats['active']}\n"

        f"  • В архиве: {stats['archived']}\n\n"

        f"⚠️ **Пропущены:**\n{fail_text}\n\n"

        f"💡 Можно сразу запустить «Рассылка по базе (ЛС)».",

        reply_markup=get_db_keyboard()

    )

    print(f"[SEARCH] [{owner_id}] keyword='{keyword}' found={total_found} added={total_added}")


async def search_global(bot: Bot, owner_id: int, keyword: str, account_name: str, status_msg: Message):

    """

    Глобальный поиск по всем чатам. Требует Premium на аккаунте-поисковике.

    """

    client = telethon_accounts.get(owner_id, {}).get(account_name)

    if client is None:

        await status_msg.edit_text(f"❌ Аккаунт **{account_name}** не подключён.")

        return


    await status_msg.edit_text(

        f"🔍 **Глобальный поиск запущен**\n\n"

        f"🔑 Ключевое слово: `{keyword}`\n"

        f"👤 Аккаунт: **{account_name}**\n"

        f"⭐ Требуется Telegram Premium на аккаунте-поисковике.\n\n"

        f"⏳ Идёт поиск по всем чатам…"

    )


    total_found = 0

    total_added = 0

    total_messages = 0

    offset_rate = 0

    offset_peer = None

    pages = 0

    max_pages = 20  # предохранитель — больше 2000 сообщений не тянем


    try:

        while pages < max_pages:

            pages += 1

            try:

                result = await client(SearchGlobalRequest(

                    q=keyword,

                    offset_rate=offset_rate,

                    offset_peer=offset_peer,

                    offset_id=0,

                    limit=100,

                ))

            except Exception as e:

                err_name = type(e).__name__

                if "premium" in str(e).lower() or "FAKE_PREMIUM_REQUIRED" in str(e) or "FILTER_SEARCH_TOO_MANY" in str(e):

                    await status_msg.edit_text(

                        f"⚠️ **Глобальный поиск недоступен**\n\n"

                        f"Причина: `{err_name}`\n\n"

                        f"💡 Обычно Telegram требует **Premium** на аккаунте-поисковике.\n"

                        f"Либо используй «Поиск по списку чатов».",

                        reply_markup=get_db_keyboard()

                    )

                    return

                raise


            messages = getattr(result, "messages", [])

            if not messages:

                break


            batch = []

            for msg in messages:

                total_messages += 1

                u = _extract_user_from_msg(msg)

                if u is None:

                    continue

                total_found += 1

                batch.append(u)

            if batch:

                added = await add_parsed_users_bulk(owner_id, batch, f"search_global:{keyword}")

                total_added += added


            if total_messages % 200 == 0 or pages == 1:

                try:

                    await status_msg.edit_text(

                        f"🔍 **Глобальный поиск…**\n"

                        f"🔑 `{keyword}`\n"

                        f"🔎 Просмотрено: {total_messages} сообщений\n"

                        f"👤 Найдено авторов: {total_found}\n"

                        f"✅ Добавлено: {total_added}"

                    )

                except Exception:

                    pass


            # Следующая страница

            offset_rate = getattr(result, "next_rate", None) or getattr(result, "rate", None) or 0

            if hasattr(result, "next_peer") and result.next_peer:

                offset_peer = result.next_peer

            if not messages or len(messages) < 100:

                break

            await asyncio.sleep(1)

    except FloodWaitError as e:

        await status_msg.edit_text(

            f"⏳ FloodWait {e.seconds}s — жду и продолжаю.\n"

            f"Пока: найдено {total_found}, добавлено {total_added}"

        )

        await asyncio.sleep(min(e.seconds, 60))

    except Exception as e:

        await status_msg.edit_text(

            f"❌ Ошибка глобального поиска: {type(e).__name__}: {e}",

            reply_markup=get_db_keyboard()

        )

        return


    stats = await get_user_db_stats(owner_id)

    await status_msg.edit_text(

        f"✅ **Глобальный поиск завершён!**\n\n"

        f"🔑 Ключевое слово: `{keyword}`\n"

        f"🔎 Просмотрено сообщений: {total_messages}\n"

        f"👤 Уникальных авторов: {total_found}\n"

        f"🆕 Добавлено в базу: {total_added}\n\n"

        f"📊 **База:**\n"

        f"  • Всего: {stats['total']}\n"

        f"  • Активных: {stats['active']}\n"

        f"  • В архиве: {stats['archived']}\n\n"

        f"💡 Можно сразу запустить «Рассылка по базе (ЛС)».",

        reply_markup=get_db_keyboard()

    )

    print(f"[SEARCH-GLOBAL] [{owner_id}] keyword='{keyword}' found={total_found} added={total_added}")


async def db_mailing_worker(bot: Bot, owner_id: int, status_msg: Message):

    """

    Шлёт ЛС по юзерам из БД. После успешной отправки помечает archived=1.

    """

    state = await get_db_mailing_state(owner_id)

    account_name = state.get("account_name")

    text = state.get("text") or ""

    include_archived = bool(state.get("include_archived"))


    client = telethon_accounts.get(owner_id, {}).get(account_name)

    if client is None:

        await save_db_mailing_state(owner_id, status="stopped")

        await status_msg.edit_text(f"❌ Аккаунт **{account_name}** недоступен. Рассылка остановлена.")

        return


    rows = await get_active_user_ids(owner_id, include_archived=include_archived, limit=100000)

    if not rows:

        await save_db_mailing_state(owner_id, status="stopped", total=0, sent=0, failed=0, current_index=0)

        await status_msg.edit_text("❌ В базе нет юзеров для рассылки. Сначала спарсите группу.", reply_markup=get_db_keyboard())

        return


    await save_db_mailing_state(owner_id, status="started", total=len(rows), sent=0, failed=0, current_index=0)

    await status_msg.edit_text(

        f"📤 **Рассылка по базе запущена**\n\n"

        f"👤 Аккаунт: **{account_name}**\n"

        f"👥 Получателей: **{len(rows)}**\n"

        f"📦 Включая архив: {'да' if include_archived else 'нет'}\n\n"

        f"⏳ Прогресс будет обновляться…"

    )


    sent = 0

    failed = 0

    for idx, (db_id, tg_user_id, username, first_name) in enumerate(rows):

        # Проверка статуса (можно остановить кнопкой)

        cur = await get_db_mailing_state(owner_id)

        if cur.get("status") != "started":

            await status_msg.edit_text(

                f"⏹ **Рассылка остановлена**\n\n"

                f"✅ Отправлено: {sent}\n"

                f"❌ Ошибок: {failed}\n"

                f"📊 Обработано: {idx}/{len(rows)}",

                reply_markup=get_db_keyboard()

            )

            return


        # Проверка дневного лимита

        config = await load_config(owner_id)

        if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:

            await save_db_mailing_state(owner_id, status="stopped")

            await status_msg.edit_text(

                f"🛑 **Дневной лимит исчерпан!**\n\n"

                f"✅ Отправлено: {sent}\n"

                f"❌ Ошибок: {failed}\n"

                f"⭐ Купите Премиум, чтобы продолжить.",

                reply_markup=get_db_keyboard()

            )

            return


        # Гарантируем коннект

        client = await _ensure_client_connected(client, account_name, owner_id)

        if client is None:

            await asyncio.sleep(5)

            client = telethon_accounts.get(owner_id, {}).get(account_name)

            if client is None:

                failed += 1

                await save_db_mailing_state(owner_id, sent=sent, failed=failed, current_index=idx+1)

                continue


        # Персонализируем сообщение

        personal = text

        if "{first_name}" in personal and first_name:

            personal = personal.replace("{first_name}", first_name)

        if "{username}" in personal:

            tag = f"@{username}" if username else (first_name or "друг")

            personal = personal.replace("{username}", tag)


        try:

            await client.send_message(tg_user_id, personal)

            sent += 1

            await mark_user_archived(db_id)

            config["stats"]["sent_count"] += 1

            config["daily_sent"] += 1

            await save_config(owner_id, config)

        except FloodWaitError as e:

            await status_msg.edit_text(

                f"⏳ **FloodWait {e.seconds}s** — аккаунт **{account_name}** замедлен Telegram.\n"

                f"Жду и продолжаю… (уже отправлено: {sent})"

            )

            await asyncio.sleep(min(e.seconds, 300))

            # Повторим эту же итерацию

            try:

                await client.send_message(tg_user_id, personal)

                sent += 1

                await mark_user_archived(db_id)

                config["stats"]["sent_count"] += 1

                config["daily_sent"] += 1

                await save_config(owner_id, config)

            except Exception as e2:

                failed += 1

                print(f"[DB-MAIL] [{owner_id}] Повтор тоже упал на {tg_user_id}: {e2}")

        except Exception as e:

            failed += 1

            err = str(e).lower()

            # Если юзер заблокировал бота/аккаунт — помечаем как архив, чтобы не долбиться

            if any(x in err for x in ["peer", "blocked", "deactivated", "user is deleted", "forbidden", "chat not found"]):

                await mark_user_archived(db_id)

            print(f"[DB-MAIL] [{owner_id}] Ошибка отправки {tg_user_id}: {type(e).__name__}: {e}")


        # Сохраняем прогресс каждые 5 отправок

        if (sent + failed) % 5 == 0:

            await save_db_mailing_state(owner_id, sent=sent, failed=failed, current_index=idx+1)

            try:

                await status_msg.edit_text(

                    f"📤 **Рассылка по базе идёт**\n\n"

                    f"✅ Отправлено: {sent}\n"

                    f"❌ Ошибок: {failed}\n"

                    f"📊 Прогресс: {idx+1}/{len(rows)}\n"

                    f"📈 {(idx+1)*100//len(rows)}%"

                )

            except Exception:

                pass


        # Задержка из настроек юзера

        delay = random.randint(config["delays"]["min"], config["delays"]["max"])

        await asyncio.sleep(delay)


    # Готово

    await save_db_mailing_state(owner_id, status="stopped", sent=sent, failed=failed, current_index=len(rows))

    await status_msg.edit_text(

        f"✅ **Рассылка по базе завершена!**\n\n"

        f"👥 Было в очереди: {len(rows)}\n"

        f"✅ Отправлено: {sent}\n"

        f"❌ Ошибок: {failed}\n"

        f"🗂 Все успешные получатели ушли в архив автоматически.",

        reply_markup=get_db_keyboard()

    )


async def _ensure_client_connected(client, acc_name: str, user_id: int):

    """Гарантирует, что клиент подключён и авторизован. Возвращает клиент или None."""

    try:

        if not client.is_connected():

            await client.connect()

        if not await client.is_user_authorized():

            print(f"⚠️ [{user_id}][{acc_name}] Сессия слетела, нужна повторная авторизация")

            try:

                await client.disconnect()

            except Exception:

                pass

            return None

        return client

    except Exception as e:

        print(f"❌ [{user_id}][{acc_name}] Не удалось подключиться: {e}")

        try:

            await client.disconnect()

        except Exception:

            pass

        return None


async def mailing_worker_for_user(user_id: int):

    """

    Воркер рассылки. Никогда не умирает от исключений.

    Просто ждёт и крутится, пока юзер не удалит аккаунт.

    """

    print(f"👷 [{user_id}] Воркер запущен")

    try:

        while True:

            try:

                config = await load_config(user_id)

                user_clients = telethon_accounts.get(user_id, {})


                if config["status"] != "started":

                    await asyncio.sleep(2)

                    continue


                if not config["chats"]:

                    print(f"⏸ [{user_id}] Нет чатов для рассылки, жду...")

                    await asyncio.sleep(5)

                    continue


                if not user_clients:

                    print(f"⏸ [{user_id}] Нет активных аккаунтов, пробую перезагрузить сессии...")

                    await init_telethon_accounts_for_user(user_id)

                    user_clients = telethon_accounts.get(user_id, {})

                    if not user_clients:

                        await asyncio.sleep(10)

                        continue


                for chat in list(config["chats"]):

                    # Проверка статуса перед каждой отправкой

                    config = await load_config(user_id)

                    if config["status"] != "started":

                        break


                    # Лимит для free-юзеров

                    if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:

                        config["status"] = "stopped"

                        await save_config(user_id, config)

                        print(f"🛑 [{user_id}] Дневной лимит исчерпан, рассылка остановлена")

                        break


                    # Пройдёмся по всем аккаунтам

                    for acc_name, client in list(user_clients.items()):

                        # Снова свежий конфиг и статус

                        config = await load_config(user_id)

                        if config["status"] != "started":

                            break

                        if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:

                            break


                        client = await _ensure_client_connected(client, acc_name, user_id)

                        if client is None:

                            continue


                        try:

                            msg_text = config["scenarios"].get(acc_name, "Привет!")

                            if not config["is_premium"]:

                                msg_text += WATERMARK


                            target = int(chat) if str(chat).lstrip('-').isdigit() else chat

                            await client.send_message(target, msg_text)


                            config["stats"]["sent_count"] += 1

                            config["daily_sent"] += 1

                            await save_config(user_id, config)

                            print(f"📤 [{user_id}][{acc_name}] → {chat} ({config['daily_sent']}/{FREE_LIMIT if not config['is_premium'] else '∞'})")


                        except FloodWaitError as e:

                            print(f"⏳ [{user_id}][{acc_name}] FloodWait {e.seconds}s — жду")

                            await asyncio.sleep(e.seconds)

                        except Exception as e:

                            print(f"❌ [{user_id}][{acc_name}] Ошибка отправки в {chat}: {type(e).__name__}: {e}")

                            # НЕ делаем disconnect на каждую ошибку — только на критичные

                            err_str = str(e).lower()

                            if "auth" in err_str or "deactivat" in err_str or "banned" in err_str or "session" in err_str:

                                try:

                                    await client.disconnect()

                                except Exception:

                                    pass


                        delay = random.randint(config["delays"]["min"], config["delays"]["max"])

                        await asyncio.sleep(delay)


                await asyncio.sleep(3)

            except asyncio.CancelledError:

                raise

            except Exception as inner_e:

                # Любая необработанная ошибка внутри — НЕ убиваем воркер

                print(f"💥 [{user_id}] Внутренняя ошибка воркера: {type(inner_e).__name__}: {inner_e}")

                await asyncio.sleep(5)

    except asyncio.CancelledError:

        print(f"🛑 [{user_id}] Воркер остановлен")

        raise

    except Exception as e:

        print(f"💀 [{user_id}] Воркер упал с критической ошибкой: {type(e).__name__}: {e}")


active_workers = {}


def start_user_worker(user_id: int):

    if user_id not in active_workers or active_workers[user_id].done():

        active_workers[user_id] = asyncio.create_task(mailing_worker_for_user(user_id))


def get_main_keyboard(status="stopped", is_premium=False, db_mailing_status="stopped"):

    if status == "started":

        status_row = [InlineKeyboardButton(text="🟢 РАССЫЛКА ИДЕТ 🟢", callback_data="ignore_click")]

        control_row = [InlineKeyboardButton(text="⏸ Пауза", callback_data="pause"), InlineKeyboardButton(text="⏹ Стоп", callback_data="stop")]

    elif status == "paused":

        status_row = [InlineKeyboardButton(text="🟡 НА ПАУЗЕ 🟡", callback_data="ignore_click")]

        control_row = [InlineKeyboardButton(text="▶️ Продолжить", callback_data="start"), InlineKeyboardButton(text="⏹ Стоп", callback_data="stop")]

    else:

        status_row = [InlineKeyboardButton(text="🔴 ОСТАНОВЛЕНО 🔴", callback_data="ignore_click")]

        control_row = [InlineKeyboardButton(text="🚀 Запустить", callback_data="start")]


    # Подсветка для активной рассылки по базе

    db_label = "📤 Рассылка по базе идёт" if db_mailing_status == "started" else "👥 Парсер / База юзеров"


    buttons = [

        status_row, control_row,

        [InlineKeyboardButton(text="🔄 Сбросить статусы", callback_data="reset_status"), InlineKeyboardButton(text="🧹 Глубокая очистка", callback_data="deep_clean")],

        [InlineKeyboardButton(text="📝 Сценарий", callback_data="scenario"), InlineKeyboardButton(text="💬 Чаты", callback_data="chats_menu")],

        [InlineKeyboardButton(text="⏱ Настроить задержки", callback_data="set_delay"), InlineKeyboardButton(text="📈 Статистика", callback_data="stats_view")],

        [InlineKeyboardButton(text="🔑 Аккаунты / Сессии", callback_data="accounts_manage")],

        [InlineKeyboardButton(text=db_label, callback_data="db_menu")],

    ]

    

    if not is_premium:

        buttons.append([InlineKeyboardButton(text="⭐ Купить Премиум ($1.5)", callback_data="buy_premium")])

        

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_db_keyboard():

    return InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="👥 Спарсить группу", callback_data="db_parse"),

         InlineKeyboardButton(text="🔍 Поиск по слову", callback_data="db_search")],

        [InlineKeyboardButton(text="📊 Статистика базы", callback_data="db_stats")],

        [InlineKeyboardButton(text="📤 Рассылка по базе (ЛС)", callback_data="db_mailing")],

        [InlineKeyboardButton(text="⏹ Остановить рассылку по базе", callback_data="db_mailing_stop")],

        [InlineKeyboardButton(text="🗂 Восстановить всех из архива", callback_data="db_unarchive_all")],

        [InlineKeyboardButton(text="💣 Удалить всю базу", callback_data="db_clear_confirm")],

        [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")],

    ])


async def build_main_kb(user_id: int):

    """Собирает главную клавиатуру с актуальным статусом рассылки по базе."""

    config = await load_config(user_id)

    db_state = await get_db_mailing_state(user_id)

    return get_main_keyboard(

        config["status"],

        config["is_premium"],

        db_state.get("status", "stopped")

    )


bot_storage = MemoryStorage()

dp = Dispatcher(storage=bot_storage)


@dp.callback_query(F.data == "ignore_click")

async def cb_ignore(callback: CallbackQuery):

    await callback.answer()


@dp.message(Command("start"))

async def cmd_start(message: Message):

    user_id = message.from_user.id

    config = await load_config(user_id)

    await message.answer("📋 **Главное меню панели управления рассылками:**", reply_markup=await build_main_kb(message.from_user.id))


@dp.callback_query(F.data == "back_to_menu")

async def cb_back_to_menu(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    await callback.message.edit_text("📋 **Главное меню панели управления рассылками:**", reply_markup=await build_main_kb(callback.from_user.id))


@dp.callback_query(F.data == "buy_premium")

async def cb_buy_premium(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if config["is_premium"]:

        await callback.answer("⭐ У вас уже есть Премиум!", show_alert=True)

        return

        

    pay_url, invoice_id, err = await create_crypto_invoice(SUB_PRICE_USD, user_id)

    if not pay_url:

        # Покажем конкретную причину — пригодится для дебага

        detail = f"\n\nПричина: {err}" if err else ""

        await callback.answer(f"❌ Ошибка платежной системы. Проверьте настройки приложения в Crypto Pay.{detail}", show_alert=True)

        return

        

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="💸 Оплатить $1.5 в CryptoBot", url=pay_url)],

        [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check_pay_{invoice_id}")],

        [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]

    ])

    await callback.message.edit_text("💎 **Покупка EgorMailer Premium**\n\n"

                                    "• Без водных знаков бота\n"

                                    "• Полное снятие лимита (более 50 писем в день)\n\n"

                                    "Нажмите кнопку ниже для перехода к оплате:", reply_markup=kb)


@dp.callback_query(F.data.startswith("check_pay_"))

async def cb_check_pay(callback: CallbackQuery, bot: Bot):

    user_id = callback.from_user.id

    invoice_id = int(callback.data.replace("check_pay_", ""))

    is_paid = await check_crypto_invoice(invoice_id)

    

    if is_paid:

        config = await load_config(user_id)

        config["is_premium"] = True

        await save_config(user_id, config)

        await callback.message.edit_text("🎉 **Поздравляем! Премиум успешно активирован!**\n"

                                        "Лимиты сняты, водный знак отключен.", 

                                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📥 В меню", callback_data="back_to_menu")]]))

        

        try:

            username = f"@{callback.from_user.username}" if callback.from_user.username else "Нет юзернейма"

            await bot.send_message(

                chat_id=ADMIN_ID,

                text=f"💰 **Новая покупка подписки!**\n\n"

                     f"• Пользователь: {callback.from_user.full_name}\n"

                     f"• Юзернейм: {username}\n"

                     f"• ID: `{user_id}`\n"

                     f"• Сумма: {SUB_PRICE_USD}$ через CryptoBot"

            )

        except Exception as e:

            print(f"Не удалось отправить уведомление админу: {e}")

    else:

        await callback.answer("❌ Оплата не найдена. Сначала оплатите счет в CryptoBot!", show_alert=True)


@dp.callback_query(F.data == "start")

async def cb_start(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    user_clients = telethon_accounts.get(user_id, {})

    

    if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:

        await callback.answer("❌ Вы исчерпали дневной лимит в 50 сообщений! Купите премиум.", show_alert=True)

        return

    if not user_clients or not config.get("session_names"):

        await callback.answer("❌ Ошибка: Нет активных аккаунтов!", show_alert=True)

        return

    if not config["chats"]:

        await callback.answer("❌ Ошибка: Список чатов пуст!", show_alert=True)

        return


    config["status"] = "started"

    await save_config(user_id, config)

    await callback.message.edit_reply_markup(reply_markup=await build_main_kb(callback.from_user.id))

    await callback.answer("🚀 Рассылка успешно запущена!", show_alert=False)

    start_user_worker(user_id)


@dp.callback_query(F.data == "pause")

async def cb_pause(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    config["status"] = "paused"

    await save_config(user_id, config)

    await callback.message.edit_reply_markup(reply_markup=await build_main_kb(callback.from_user.id))

    await callback.answer("⏸ Поставлено на паузу.", show_alert=False)


@dp.callback_query(F.data == "stop")

async def cb_stop(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    config["status"] = "stopped"

    await save_config(user_id, config)

    await callback.message.edit_reply_markup(reply_markup=await build_main_kb(callback.from_user.id))

    await callback.answer("⏹ Рассылка остановлена.", show_alert=False)


@dp.callback_query(F.data == "reset_status")

async def cb_reset_status(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    config["status"] = "stopped"

    await save_config(user_id, config)

    await callback.message.edit_reply_markup(reply_markup=await build_main_kb(callback.from_user.id))

    await callback.answer("🔄 Статус сброшен.", show_alert=True)


@dp.callback_query(F.data == "deep_clean")

async def cb_deep_clean(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    config["chats"] = []

    config["scenarios"] = {}

    config["status"] = "stopped"

    config["stats"]["sent_count"] = 0

    await save_config(user_id, config)

    await callback.message.edit_reply_markup(reply_markup=await build_main_kb(callback.from_user.id))

    await callback.answer("🧹 Данные полностью очищены.", show_alert=True)


@dp.callback_query(F.data == "scenario")

async def cb_scenario(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    text = "📝 **Сценарии для аккаунтов:**\n\n"

    for acc in config.get("session_names", []):

        text += f"• **{acc}**: {config['scenarios'].get(acc, 'По умолчанию')}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✏️ Изменить", callback_data="edit_scenario")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])

    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "edit_scenario")

async def cb_edit_scenario(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if not config.get("session_names"):

        await callback.answer("❌ Нет активных аккаунтов!", show_alert=True)

        return

    buttons = [[InlineKeyboardButton(text=acc, callback_data=f"sc_acc_{acc}")] for acc in config["session_names"]]

    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")])

    await callback.message.edit_text("Выберите аккаунт:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@dp.callback_query(F.data.startswith("sc_acc_"))

async def cb_select_sc_acc(callback: CallbackQuery, state: FSMContext):

    acc_name = callback.data.replace("sc_acc_", "")

    await state.update_data(target_acc=acc_name)

    await state.set_state(BotStates.waiting_for_scenario_text)

    await callback.message.answer(f"📝 Введите новый текст для **{acc_name}**:")


@dp.message(BotStates.waiting_for_scenario_text)

async def process_sc_text(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    acc_name = data.get("target_acc")

    config = await load_config(user_id)

    config["scenarios"][acc_name] = message.text

    await save_config(user_id, config)

    await message.answer(f"✅ Текст для **{acc_name}** обновлен!", reply_markup=await build_main_kb(user_id))

    await state.clear()


@dp.callback_query(F.data == "chats_menu")

async def cb_chats(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    chats_list = "\n".join([f"• {c}" for c in config["chats"]]) if config["chats"] else "Список пуст"

    text = f"💬 **Чаты ({len(config['chats'])}):**\n\n{chats_list}"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Импортировать", callback_data="import_chats")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])

    await callback.message.edit_text(text, reply_markup=kb)


# ============================================================

#   ХЕНДЛЕРЫ: ПАРСЕР / БАЗА / РАССЫЛКА ПО БАЗЕ

# ============================================================


@dp.callback_query(F.data == "db_menu")

async def cb_db_menu(callback: CallbackQuery):

    user_id = callback.from_user.id

    stats = await get_user_db_stats(user_id)

    state = await get_db_mailing_state(user_id)

    mailing_status = state.get("status", "stopped")

    text = (

        f"👥 **Парсер / База юзеров**\n\n"

        f"📊 В базе: **{stats['total']}** (активных: {stats['active']}, в архиве: {stats['archived']})\n"

        f"📦 Источников-групп: {stats['groups']}\n"

        f"📤 Рассылка по базе: **{mailing_status}**\n\n"

        f"Выберите действие:"

    )

    await callback.message.edit_text(text, reply_markup=get_db_keyboard())


@dp.callback_query(F.data == "db_stats")

async def cb_db_stats(callback: CallbackQuery):

    user_id = callback.from_user.id

    stats = await get_user_db_stats(user_id)

    text = (

        f"📊 **Статистика вашей базы:**\n\n"

        f"👥 Всего юзеров: **{stats['total']}**\n"

        f"✅ Активных (доступны для рассылки): **{stats['active']}**\n"

        f"🗂 В архиве: **{stats['archived']}**\n"

        f"🤖 Ботов отфильтровано: {stats['bots']}\n"

        f"📦 Уникальных групп-источников: {stats['groups']}"

    )

    await callback.message.edit_text(text, reply_markup=get_db_keyboard())


@dp.callback_query(F.data == "db_search")

async def cb_db_search(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if not config.get("session_names"):

        await callback.answer("❌ Сначала добавьте аккаунт!", show_alert=True)

        return

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="🌐 Глобально (по всем чатам, нужен Premium)", callback_data="db_search_global")],

        [InlineKeyboardButton(text="📋 По списку чатов", callback_data="db_search_in_groups")],

        [InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")],

    ])

    await callback.message.edit_text(

        "🔍 **Поиск по ключевому слову**\n\n"

        "Ищет сообщения с указанным словом и собирает `user_id` авторов.\n\n"

        "📌 **Глобально** — по всем чатам аккаунта. Работает только если аккаунт-поисковик имеет **Telegram Premium**.\n\n"

        "📋 **По списку чатов** — ищет только в тех чатах, которые вы укажете. Работает на любом аккаунте.\n\n"

        "Что хотите сделать?",

        reply_markup=kb

    )


@dp.callback_query(F.data == "db_search_global")

async def cb_db_search_global(callback: CallbackQuery, state: FSMContext):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    await state.set_state(BotStates.waiting_for_search_keyword)

    await state.update_data(search_mode="global")

    if len(config["session_names"]) > 1:

        # Запомним пока что выбор первого, но ниже дадим сменить через аккаунт

        await state.update_data(search_account=config["session_names"][0])

    elif config.get("session_names"):

        await state.update_data(search_account=config["session_names"][0])

    await callback.message.edit_text(

        "🔑 **Глобальный поиск: введите ключевое слово**\n\n"

        "Например: `купить бота`, `ищу фрилансера`, `нужен парсер`\n\n"

        "После слова — выберете аккаунт-поисковик (если их несколько).",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_search")]

        ])

    )


@dp.callback_query(F.data == "db_search_in_groups")

async def cb_db_search_in_groups(callback: CallbackQuery, state: FSMContext):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    await state.set_state(BotStates.waiting_for_search_keyword)

    await state.update_data(search_mode="in_groups")

    if config.get("session_names"):

        await state.update_data(search_account=config["session_names"][0])

    await callback.message.edit_text(

        "🔑 **Поиск по списку чатов: введите ключевое слово**\n\n"

        "Например: `купить бота`, `ищу фрилансера`, `нужен парсер`\n\n"

        "После слова — спрошу список чатов для поиска.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_search")]

        ])

    )


@dp.message(BotStates.waiting_for_search_keyword)

async def process_search_keyword(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    mode = data.get("search_mode")

    account_name = data.get("search_account")

    keyword = (message.text or "").strip()


    if not keyword:

        await message.answer("❌ Пустое слово. Пришлите ключевое слово.")

        return

    if len(keyword) < 2:

        await message.answer("❌ Слишком короткое. Минимум 2 символа.")

        return


    config = await load_config(user_id)


    # Если несколько аккаунтов — спросим какой использовать

    if len(config.get("session_names", [])) > 1 and not message.text.startswith("/"):

        buttons = [[InlineKeyboardButton(text=a, callback_data=f"db_sacc_{mode}_{a}")] for a in config["session_names"]]

        # Сохраним в state keyword и пойдём в инлайн-обработчик

        await state.update_data(search_keyword=keyword)

        await message.answer(

            f"🔑 Слово: `{keyword}`\n\n👤 Выберите аккаунт-поисковик:",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)

        )

        return


    # Один аккаунт — идём дальше

    if mode == "global":

        status_msg = await message.answer(

            f"⏳ Запускаю глобальный поиск по слову `{keyword}` с аккаунта **{account_name}**…"

        )

        task = asyncio.create_task(search_global(message.bot, user_id, keyword, account_name, status_msg))

        active_searches[user_id] = task

        await state.clear()

    else:  # in_groups

        await state.update_data(search_keyword=keyword)

        await state.set_state(BotStates.waiting_for_search_groups)

        await message.answer(

            f"🔑 Слово: `{keyword}`\n\n"

            f"📝 Теперь отправьте список чатов для поиска (по одному на строку):\n"

            f"• @username\n"

            f"• https://t.me/username\n"

            f"• -100xxxxxxxxxx (id)\n\n"

            f"💡 Аккаунт-поисковик должен быть участником этих чатов.",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="⬅ Назад", callback_data="db_search")]

            ])

        )


@dp.callback_query(F.data.startswith("db_sacc_"))

async def cb_db_search_acc_choose(callback: CallbackQuery, state: FSMContext):

    # db_sacc_<mode>_<acc_name>

    rest = callback.data.replace("db_sacc_", "", 1)

    # rest = "global_session_name" или "in_groups_session_name"

    if rest.startswith("global_"):

        mode = "global"

        account_name = rest[len("global_"):]

    elif rest.startswith("in_groups_"):

        mode = "in_groups"

        account_name = rest[len("in_groups_"):]

    else:

        await callback.answer("❌ Ошибка выбора.", show_alert=True)

        return


    data = await state.get_data()

    keyword = data.get("search_keyword")

    user_id = callback.from_user.id


    if not keyword:

        await state.clear()

        await callback.message.edit_text("❌ Ключевое слово потеряно, начните заново.", reply_markup=get_db_keyboard())

        return


    if mode == "global":

        status_msg = await callback.message.edit_text(

            f"⏳ Запускаю глобальный поиск по `{keyword}` с **{account_name}**…"

        )

        task = asyncio.create_task(search_global(callback.message.bot, user_id, keyword, account_name, status_msg))

        active_searches[user_id] = task

        await state.clear()

    else:

        await state.update_data(search_account=account_name, search_keyword=keyword)

        await state.set_state(BotStates.waiting_for_search_groups)

        await callback.message.edit_text(

            f"🔑 Слово: `{keyword}`\n"

            f"👤 Аккаунт: **{account_name}**\n\n"

            f"📝 Отправьте список чатов для поиска (по одному на строку):\n"

            f"• @username\n"

            f"• https://t.me/username\n"

            f"• -100xxxxxxxxxx (id)",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="⬅ Назад", callback_data="db_search")]

            ])

        )


@dp.message(BotStates.waiting_for_search_groups)

async def process_search_groups(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    keyword = data.get("search_keyword")

    account_name = data.get("search_account")

    if not keyword or not account_name:

        await state.clear()

        await message.answer("❌ Контекст потерян, начните заново через ‘Поиск по слову’.")

        return


    raw = [line.strip() for line in (message.text or "").split("\n") if line.strip()]

    groups = []

    for line in raw:

        if "t.me/" in line:

            line = "@" + line.split("t.me/")[-1].replace("/", "").replace("+", "")

        groups.append(line)

    if not groups:

        await message.answer("❌ Пустой список. Пришлите хотя бы один чат.")

        return


    status_msg = await message.answer(

        f"⏳ Запускаю поиск `{keyword}` по {len(groups)} чатам с **{account_name}**…"

    )

    task = asyncio.create_task(search_in_groups(message.bot, user_id, keyword, groups, account_name, status_msg))

    active_searches[user_id] = task

    await state.clear()


@dp.callback_query(F.data == "db_parse")

async def cb_db_parse(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if not config.get("session_names"):

        await callback.answer("❌ Сначала добавьте аккаунт через ‘Аккаунты / Сессии’!", show_alert=True)

        return

    if len(config["session_names"]) == 1:

        # Один аккаунт — сразу просим группы

        acc = config["session_names"][0]

        await state_dispatch_parse(callback.message, user_id, acc)

        return

    # Несколько — спросим какой использовать для парсинга

    buttons = [[InlineKeyboardButton(text=a, callback_data=f"db_parse_acc_{a}")] for a in config["session_names"]]

    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")])

    await callback.message.edit_text(

        "👥 **Парсер: выберите аккаунт-парсер**\n\n"

        "С этого аккаунта будут читаться участники групп.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)

    )


async def state_dispatch_parse(message, user_id, account_name):

    state = dp.fsm.get_context(bot=message.bot, chat_id=user_id, user_id=user_id)

    await state.set_state(BotStates.waiting_for_parse_groups)

    await state.update_data(parse_account=account_name)

    await message.answer(

        f"👥 **Аккаунт-парсер:** {account_name}\n\n"

        f"📝 Отправьте список групп (по одной на строку):\n"

        f"• @username\n"

        f"• https://t.me/username\n"

        f"• -100xxxxxxxxxx (id)\n\n"

        f"⏳ Большие группы = дольше + риск FloodWait. Лучше 5–10 штук за раз.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")]

        ])

    )


@dp.callback_query(F.data.startswith("db_parse_acc_"))

async def cb_db_parse_acc_choose(callback: CallbackQuery, state: FSMContext):

    acc = callback.data.replace("db_parse_acc_", "")

    await state_dispatch_parse(callback.message, callback.from_user.id, acc)


@dp.message(BotStates.waiting_for_parse_groups)

async def process_parse_groups(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    account_name = data.get("parse_account")


    raw = [line.strip() for line in (message.text or "").split("\n") if line.strip()]

    groups = []

    for line in raw:

        if "t.me/" in line:

            line = "@" + line.split("t.me/")[-1].replace("/", "").replace("+", "")

        groups.append(line)


    if not groups:

        await message.answer("❌ Пустой список. Пришлите хотя бы одну группу.")

        return

    if not account_name:

        await state.clear()

        await message.answer("❌ Не выбран аккаунт. Начните заново через ‘Парсер / База’.")

        return


    status_msg = await message.answer(f"⏳ Готовлю парсинг: {len(groups)} групп, аккаунт **{account_name}**…")

    task = asyncio.create_task(parse_users_from_groups(message.bot, user_id, groups, account_name, status_msg))

    active_parsers[user_id] = task

    await state.clear()


@dp.callback_query(F.data == "db_mailing")

async def cb_db_mailing(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if not config.get("session_names"):

        await callback.answer("❌ Сначала добавьте аккаунт!", show_alert=True)

        return

    state = await get_db_mailing_state(user_id)

    if state.get("status") == "started":

        await callback.answer("⏳ Рассылка по базе уже идёт. Можно остановить кнопкой ниже.", show_alert=True)

        return

    stats = await get_user_db_stats(user_id)

    if stats["active"] == 0:

        await callback.answer("❌ В базе нет активных юзеров. Сначала спарсите группу.", show_alert=True)

        return

    if len(config["session_names"]) == 1:

        await state_dispatch_db_mailing_text(callback.message, user_id, config["session_names"][0])

        return

    buttons = [[InlineKeyboardButton(text=a, callback_data=f"db_mail_acc_{a}")] for a in config["session_names"]]

    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")])

    await callback.message.edit_text(

        f"📤 **Рассылка по базе: выберите аккаунт-отправитель**\n\n"

        f"В базе: {stats['active']} активных юзеров.\n"

        f"С этого аккаунта будут отправляться ЛС.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)

    )


async def state_dispatch_db_mailing_text(message, user_id, account_name):

    state = dp.fsm.get_context(bot=message.bot, chat_id=user_id, user_id=user_id)

    await state.set_state(BotStates.waiting_for_db_mailing_text)

    await state.update_data(mail_account=account_name)

    await message.answer(

        f"📤 **Аккаунт-отправитель:** {account_name}\n\n"

        f"📝 Отправьте текст рассылки.\n\n"

        f"💡 Поддерживаются плейсхолдеры:\n"

        f"  • `{{first_name}}` — имя получателя\n"

        f"  • `{{username}}` — @username или имя\n\n"

        f"⚠️ **Обратите внимание:** Telegram ограничивает ЛС незнакомым юзерам. "

        f"Многие сообщения не дойдут, а аккаунт могут заблокировать за спам. "

        f"Рассылайте только тем, кто мог дать согласие.\n\n"

        f"После отправки текста начнётся рассылка, а юзеры автоматически уйдут в архив.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")]

        ])

    )


@dp.callback_query(F.data.startswith("db_mail_acc_"))

async def cb_db_mail_acc_choose(callback: CallbackQuery, state: FSMContext):

    acc = callback.data.replace("db_mail_acc_", "")

    await state_dispatch_db_mailing_text(callback.message, callback.from_user.id, acc)


@dp.message(BotStates.waiting_for_db_mailing_text)

async def process_db_mailing_text(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    account_name = data.get("mail_account")

    text = message.text or ""

    if not account_name:

        await state.clear()

        await message.answer("❌ Не выбран аккаунт. Начните заново.")

        return

    if not text.strip():

        await message.answer("❌ Текст пустой. Пришлите сообщение.")

        return


    # Проверка лимита перед стартом

    config = await load_config(user_id)

    if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:

        await message.answer("🛑 Дневной лимит уже исчерпан. Купите Премиум.")

        await state.clear()

        return


    # Сохраняем состояние и запускаем воркер

    await save_db_mailing_state(user_id, status="starting", text=text, account_name=account_name, include_archived=0)

    status_msg = await message.answer(

        f"⏳ Запускаю рассылку по базе с аккаунта **{account_name}**…"

    )

    task = asyncio.create_task(db_mailing_worker(message.bot, user_id, status_msg))

    active_db_mailings[user_id] = task

    await state.clear()


@dp.callback_query(F.data == "db_mailing_stop")

async def cb_db_mailing_stop(callback: CallbackQuery):

    user_id = callback.from_user.id

    state = await get_db_mailing_state(user_id)

    if state.get("status") != "started":

        await callback.answer("⏹ Рассылка по базе сейчас не идёт.", show_alert=False)

        return

    await save_db_mailing_state(user_id, status="stopped")

    await callback.answer("⏹ Останавливаю рассылку по базе…", show_alert=True)

    # Воркер сам заметит и закроет статус-сообщение


@dp.callback_query(F.data == "db_unarchive_all")

async def cb_db_unarchive_all(callback: CallbackQuery):

    user_id = callback.from_user.id

    n = await unarchive_all_for_owner(user_id)

    await callback.answer(f"♻️ Снято с архива: {n} юзеров", show_alert=True)

    await cb_db_menu(callback)


@dp.callback_query(F.data == "db_clear_confirm")

async def cb_db_clear_confirm(callback: CallbackQuery):

    user_id = callback.from_user.id

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="💣 Да, удалить ВСЮ базу", callback_data="db_clear_do")],

        [InlineKeyboardButton(text="⬅ Отмена", callback_data="db_menu")],

    ])

    await callback.message.edit_text(

        "⚠️ **Точно удалить базу?**\n\n"

        "Это сотрёт всех спарсенных юзеров и архив. Действие необратимо.",

        reply_markup=kb

    )


@dp.callback_query(F.data == "db_clear_do")

async def cb_db_clear_do(callback: CallbackQuery):

    user_id = callback.from_user.id

    await clear_all_for_owner(user_id)

    await callback.answer("🧹 База очищена", show_alert=True)

    await cb_db_menu(callback)


@dp.callback_query(F.data == "import_chats")

async def cb_import_chats(callback: CallbackQuery, state: FSMContext):

    await state.set_state(BotStates.waiting_for_chats)

    await callback.message.answer("📝 Отправьте список чатов (каждый с новой строки):")


@dp.message(BotStates.waiting_for_chats)

async def process_chats_list(message: Message, state: FSMContext):

    user_id = message.from_user.id

    lines = message.text.split("\n")

    cleaned = []

    for line in lines:

        chat = line.strip()

        if not chat: continue

        cleaned.append("@" + chat.split("t.me/")[-1].replace("+", "") if "t.me/" in chat else chat)

    config = await load_config(user_id)

    config["chats"] = list(set(config["chats"] + cleaned))

    await save_config(user_id, config)

    await message.answer(f"✅ Всего: {len(config['chats'])} чатов загружено.", reply_markup=await build_main_kb(user_id))

    await state.clear()


@dp.callback_query(F.data == "set_delay")

async def cb_set_delay(callback: CallbackQuery, state: FSMContext):

    await state.set_state(BotStates.waiting_for_delay_min)

    await callback.message.answer("⏱ Введите **минимальную** задержку в секундах:")


@dp.message(BotStates.waiting_for_delay_min)

async def process_delay_min(message: Message, state: FSMContext):

    if not message.text.isdigit():

        await message.answer("❌ Введите число:")

        return

    await state.update_data(min_delay=int(message.text))

    await state.set_state(BotStates.waiting_for_delay_max)

    await message.answer("⏱ Введите **максимальную** задержку в секундах:")


@dp.message(BotStates.waiting_for_delay_max)

async def process_delay_max(message: Message, state: FSMContext):

    if not message.text.isdigit():

        await message.answer("❌ Введите число:")

        return

    user_id = message.from_user.id

    data = await state.get_data()

    min_delay = data.get("min_delay")

    max_delay = int(message.text)

    

    if min_delay > max_delay:

        await message.answer("❌ Ошибка. Начните заново.")

        await state.clear()

        return


    config = await load_config(user_id)

    config["delays"]["min"] = min_delay

    config["delays"]["max"] = max_delay

    await save_config(user_id, config)

    await message.answer(f"✅ Задержки обновлены: **{min_delay}-{max_delay} сек.**", reply_markup=await build_main_kb(user_id))

    await state.clear()


@dp.callback_query(F.data == "accounts_manage")

async def cb_accounts_manage(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    user_clients = telethon_accounts.get(user_id, {})

    

    text = "🔑 **Управление вашими сессиями:**\n\n"

    if config.get("session_names"):

        text += "\n".join([f"• {s}: {'✅ Активен' if s in user_clients else '❌ Отключен'}" for s in config["session_names"]])

    else:

        text += "Нет добавленных аккаунтов."

        

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить", callback_data="add_new_account")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])

    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "add_new_account")

async def cb_add_account(callback: CallbackQuery, state: FSMContext):

    await state.set_state(BotStates.waiting_for_phone)

    await callback.message.answer("📱 Введите телефон:")


@dp.message(BotStates.waiting_for_phone)

async def process_phone(message: Message, state: FSMContext):

    user_id = message.from_user.id

    phone = message.text.strip().replace(" ", "")

    config = await load_config(user_id)

    

    new_session = f"session_{user_id}_{len(config.get('session_names', [])) + 1}"

    client = TelegramClient(new_session, API_ID, API_HASH)

    await client.connect()

    try:

        token = await client.send_code_request(phone)

        active_auths[user_id] = {"client": client, "phone": phone, "token": token, "session_name": new_session}

        await state.set_state(BotStates.waiting_for_code)

        await message.answer("📩 Введите код:")

    except Exception as e:

        await message.answer(f"❌ Ошибка: {e}")

        await state.clear()


@dp.message(BotStates.waiting_for_code)

async def process_code(message: Message, state: FSMContext):

    user_id = message.from_user.id

    code = "".join(filter(str.isdigit, message.text))

    if not code:

        await message.answer("❌ Введите еще раз:")

        return

    auth_data = active_auths.get(user_id)

    if not auth_data: return

    try:

        await auth_data["client"].sign_in(auth_data["phone"], code, phone_code_hash=auth_data["token"].phone_code_hash)

        

        if user_id not in telethon_accounts:

            telethon_accounts[user_id] = {}

            

        telethon_accounts[user_id][auth_data["session_name"]] = auth_data["client"]

        config = await load_config(user_id)

        config["session_names"].append(auth_data["session_name"])

        await save_config(user_id, config)

        

        await message.answer("✅ Аккаунт добавлен!", reply_markup=await build_main_kb(user_id))

        await state.clear()

        active_auths.pop(user_id, None)

    except SessionPasswordNeededError:

        await state.set_state(BotStates.waiting_for_password)

        await message.answer("🔒 Введите пароль (2FA):")

    except Exception as e:

        await message.answer(f"❌ Ошибка: {e}")


@dp.message(BotStates.waiting_for_password)

async def process_password(message: Message, state: FSMContext):

    user_id = message.from_user.id

    auth_data = active_auths.get(user_id)

    if not auth_data: return

    try:

        await auth_data["client"].sign_in(password=message.text.strip())

        if user_id not in telethon_accounts:

            telethon_accounts[user_id] = {}

        telethon_accounts[user_id][auth_data["session_name"]] = auth_data["client"]

        config = await load_config(user_id)

        config["session_names"].append(auth_data["session_name"])

        await save_config(user_id, config)

        await message.answer("✅ Аккаунт добавлен!", reply_markup=await build_main_kb(user_id))

        await state.clear()

        active_auths.pop(user_id, None)

    except Exception as e:

        await message.answer(f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "stats_view")

async def cb_stats_view(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    

    plan = "⭐ Premium (Бесконечно)" if config["is_premium"] else f"БЕСПЛАТНЫЙ ({config['daily_sent']}/{FREE_LIMIT} писем сегодня)"

    

    text = (f"📊 **Ваша статистика:**\n\n"

            f"• Ваш тариф: **{plan}**\n"

            f"• Всего отправлено: {config['stats']['sent_count']}\n"

            f"• Текущая задержка: {config['delays']['min']}-{config['delays']['max']} сек.\n"

            f"• Загружено чатов: {len(config['chats'])}\n"

            f"• Активных аккаунтов: {len(config.get('session_names', []))}")

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])

    await callback.message.edit_text(text, reply_markup=kb)


async def main():

    bot = Bot(token=BOT_TOKEN)

    print("🗄 Инициализация БД...")

    await init_db()

    print("📂 Инициализация существующих аккаунтов пользователей...")

    await init_all_existing_accounts()

    

    for user_id in telethon_accounts.keys():

        config = await load_config(user_id)

        if config.get("status") == "started":

            start_user_worker(user_id)

            

    print("🚀 Бот успешно запущен!")

    await dp.start_polling(bot)


if __name__ == "__main__":

    asyncio.run(main())

