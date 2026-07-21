import asyncio

import json

import os

import random

import re

from aiogram import Bot, Dispatcher, F

from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from aiogram.filters import Command

from aiogram.fsm.context import FSMContext

from aiogram.fsm.state import State, StatesGroup

from aiogram.fsm.storage.memory import MemoryStorage

from telethon import TelegramClient

from telethon.errors import FloodWaitError, SessionPasswordNeededError, ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError

from telethon.tl.types import User, PeerUser

import aiosqlite


# Глобальные настройки API

API_ID = 32155028

API_HASH = "ec906474420c7cc518e2245d5829924a"

BOT_TOKEN = "7860968550:AAHNx_mJHsDrohp0DV60eTy1wCdl8gKxqmE"


# Настройки

ADMIN_ID = 7521801228

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

    waiting_for_add_users = State()

    waiting_for_db_mailing_text = State()


async def load_config(user_id: int) -> dict:

    file_path = get_user_config_path(user_id)

    async with config_lock:

        if not os.path.exists(file_path):

            with open(file_path, "w", encoding="utf-8") as f:

                json.dump(get_default_config(), f, ensure_ascii=False, indent=4)

        with open(file_path, "r", encoding="utf-8") as f:

            data = json.load(f)

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

                display_name TEXT,

                source_group TEXT,

                is_bot INTEGER DEFAULT 0,

                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                archived INTEGER DEFAULT 0,

                archived_at TIMESTAMP,

                UNIQUE(owner_id, user_id)

            )

        """)

        # Миграция: если таблица уже существовала без display_name, добавим колонку

        try:

            await db.execute("ALTER TABLE parsed_users ADD COLUMN display_name TEXT")

        except Exception:

            pass

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

    """Возвращает список (db_id, user_id, username, first_name, display_name) для рассылки."""

    sql = "SELECT id, user_id, username, first_name, display_name FROM parsed_users WHERE owner_id=?"

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


# ============================================================
#   РУЧНОЕ ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ В БАЗУ
# ============================================================


_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")


def parse_user_line(line: str) -> tuple[str, str] | None:
    """
    Парсит строку формата '@username Имя Фамилия' или '@username'.
    Возвращает (username, display_name) или None, если строка невалидная.
    """
    line = line.strip()
    if not line:
        return None
    parts = line.split(maxsplit=1)
    username = parts[0].lstrip("@").strip()
    if not _USERNAME_RE.match(username):
        return None
    display_name = parts[1].strip() if len(parts) > 1 else ""
    return username, display_name


async def add_user_to_base(owner_id: int, user_id: int, username: str,
                            first_name: str, last_name: str, display_name: str) -> None:
    """Добавляет (или обновляет) юзера в базе."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO parsed_users
               (owner_id, user_id, username, first_name, last_name, display_name, source_group, is_bot, archived)
               VALUES (?, ?, ?, ?, ?, ?, 'manual', 0, 0)
               ON CONFLICT(owner_id, user_id) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 last_name=excluded.last_name,
                 display_name=excluded.display_name,
                 archived=0,
                 archived_at=NULL""",
            (owner_id, user_id, username, first_name, last_name, display_name or None)
        )
        await db.commit()


async def resolve_and_add_user(owner_id: int, username: str, display_name: str,
                                account_name: str) -> tuple[bool, str]:
    """
    Резолвит @username через telethon и добавляет в базу.
    Возвращает (ok, message).
    """
    client = telethon_accounts.get(owner_id, {}).get(account_name)
    if client is None:
        return False, f"Аккаунт {account_name} не подключён"

    try:
        client = await _ensure_client_connected(client, account_name, owner_id)
        if client is None:
            return False, f"Не удалось подключиться к {account_name}"
        entity = await client.get_entity(username)
    except UsernameNotOccupiedError:
        return False, f"❌ @{username} — не существует"
    except UsernameInvalidError:
        return False, f"❌ @{username} — невалидный username"
    except Exception as e:
        return False, f"❌ @{username} — ошибка: {type(e).__name__}: {e}"

    if not isinstance(entity, User):
        return False, f"❌ @{username} — это не пользователь"
    if getattr(entity, "bot", False):
        return False, f"❌ @{username} — это бот"

    await add_user_to_base(
        owner_id=owner_id,
        user_id=entity.id,
        username=entity.username or username,
        first_name=entity.first_name or "",
        last_name=entity.last_name or "",
        display_name=display_name,
    )

    shown = display_name or entity.first_name or entity.username or username
    return True, f"✅ @{entity.username or username} → {shown} добавлен(а)"


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

#   РАССЫЛКА ПО БАЗЕ (ЛС каждому юзеру → автоархив)

# ============================================================


active_db_mailings = {}  # owner_id -> asyncio.Task


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

    for idx, (db_id, tg_user_id, username, first_name, display_name) in enumerate(rows):

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


        # Подгружаем конфиг (нужны только задержки и счётчик отправок)

        config = await load_config(owner_id)


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

        if "{user}" in personal:

            user_name = (display_name or first_name

                         or (f"@{username}" if username else "друг"))

            personal = personal.replace("{user}", user_name)

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


                    # Пройдёмся по всем аккаунтам

                    for acc_name, client in list(user_clients.items()):

                        # Снова свежий конфиг и статус

                        config = await load_config(user_id)

                        if config["status"] != "started":

                            break


                        client = await _ensure_client_connected(client, acc_name, user_id)

                        if client is None:

                            continue


                        try:

                            msg_text = config["scenarios"].get(acc_name, "Привет!")


                            target = int(chat) if str(chat).lstrip('-').isdigit() else chat

                            await client.send_message(target, msg_text)


                            config["stats"]["sent_count"] += 1

                            await save_config(user_id, config)

                            print(f"📤 [{user_id}][{acc_name}] → {chat} (всего: {config['stats']['sent_count']})")


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


def get_main_keyboard(status="stopped", db_mailing_status="stopped"):

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

    db_label = "📤 Рассылка по базе идёт" if db_mailing_status == "started" else "👥 База получателей"


    buttons = [

        status_row, control_row,

        [InlineKeyboardButton(text="🔄 Сбросить статусы", callback_data="reset_status"), InlineKeyboardButton(text="🧹 Глубокая очистка", callback_data="deep_clean")],

        [InlineKeyboardButton(text="📝 Сценарий", callback_data="scenario"), InlineKeyboardButton(text="💬 Чаты", callback_data="chats_menu")],

        [InlineKeyboardButton(text="⏱ Настроить задержки", callback_data="set_delay"), InlineKeyboardButton(text="📈 Статистика", callback_data="stats_view")],

        [InlineKeyboardButton(text="🔑 Аккаунты / Сессии", callback_data="accounts_manage")],

        [InlineKeyboardButton(text=db_label, callback_data="db_menu")],

    ]


    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_db_keyboard():

    return InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="➕ Добавить пользователей", callback_data="db_add_users")],

        [InlineKeyboardButton(text="📊 Статистика базы", callback_data="db_stats"),

         InlineKeyboardButton(text="📋 Список", callback_data="db_list_users")],

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


@dp.callback_query(F.data == "start")

async def cb_start(callback: CallbackQuery):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    user_clients = telethon_accounts.get(user_id, {})

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

        f"👥 **База получателей**\n\n"

        f"📊 В базе: **{stats['total']}** (активных: {stats['active']}, в архиве: {stats['archived']})\n"

        f"📤 Рассылка по базе: **{mailing_status}**\n\n"

        f"Добавляйте пользователей вручную с именами (для плейсхолдера `{{user}}`) и запускайте рассылку."

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

        f"🗂 В архиве: **{stats['archived']}**"

    )

    await callback.message.edit_text(text, reply_markup=get_db_keyboard())


@dp.callback_query(F.data == "db_list_users")

async def cb_db_list_users(callback: CallbackQuery):

    user_id = callback.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(

            "SELECT user_id, username, first_name, display_name, archived "

            "FROM parsed_users WHERE owner_id=? ORDER BY id DESC LIMIT 200",

            (user_id,)

        )

        rows = await cur.fetchall()

    if not rows:

        await callback.message.edit_text(

            "📋 **Список пуст.**\n\nДобавьте пользователей кнопкой «➕ Добавить пользователей».",

            reply_markup=get_db_keyboard()

        )

        return

    lines = []

    for tg_id, uname, fname, disp, archived in rows:

        if archived:

            mark = "🗂"

        else:

            mark = "✅"

        name = disp or fname or (f"@{uname}" if uname else "—")

        uname_part = f" (@{uname})" if uname and disp else (f" (@{uname})" if uname else "")

        lines.append(f"{mark} {name}{uname_part}  `id:{tg_id}`")

    text = "📋 **Список получателей (последние 200):**\n\n" + "\n".join(lines)

    kb_rows = [[InlineKeyboardButton(text="🗑 Удалить по id", callback_data="db_delete_prompt")]]

    kb_rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@dp.callback_query(F.data == "db_delete_prompt")

async def cb_db_delete_prompt(callback: CallbackQuery, state: FSMContext):

    await state.set_state(BotStates.waiting_for_add_users)  # переиспользуем стейт для ввода

    await state.update_data(delete_mode=True)

    await callback.message.edit_text(

        "🗑 **Удаление по id**\n\n"

        "Отправьте Telegram `user_id` (число) или несколько id через пробел/с новой строки — "

        "эти юзеры будут удалены из вашей базы.\n\n"

        "💡 Id можно посмотреть в ‘📋 Список’ — в конце строки `id:xxxx`.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_list_users")]

        ])

    )


@dp.callback_query(F.data == "db_add_users")

async def cb_db_add_users(callback: CallbackQuery, state: FSMContext):

    user_id = callback.from_user.id

    config = await load_config(user_id)

    if not config.get("session_names"):

        await callback.answer("❌ Сначала добавьте аккаунт через ‘Аккаунты / Сессии’!", show_alert=True)

        return

    await state.set_state(BotStates.waiting_for_add_users)

    await state.update_data(delete_mode=False)

    account_name = config["session_names"][0]

    if len(config["session_names"]) > 1:

        kb = [[InlineKeyboardButton(text=a, callback_data=f"add_acc_{a}")] for a in config["session_names"]]

        kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")])

        await callback.message.edit_text(

            "👤 **Выберите аккаунт** для резолва username → user_id:",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)

        )

        return

    await state.update_data(add_account=account_name)

    await callback.message.edit_text(

        f"👤 Аккаунт-резолвер: **{account_name}**\n\n"

        f"➕ **Добавление пользователей в базу**\n\n"

        f"Отправьте список, по одному на строку:\n\n"

        f"**Формат:**\n"

        f"• `@username Имя Фамилия` — добавить с именем (плейсхолдер `{{user}}` подставит это имя)\n"

        f"• `@username` — без имени (подставится имя из Telegram)\n\n"

        f"**Пример:**\n"

        f"`@marina_petrova Марина`\n"

        f"`@ivan_ivanov Иван Иванов`\n"

        f"`@someone`\n\n"

        f"💡 Бот проверит, что username существует, и сохранит в базу.\n"

        f"🔁 Повторное добавление того же юзера обновит его имя и снимет с архива.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")]

        ])

    )


@dp.callback_query(F.data.startswith("add_acc_"))

async def cb_db_add_acc_choose(callback: CallbackQuery, state: FSMContext):

    acc = callback.data.replace("add_acc_", "")

    await state.set_state(BotStates.waiting_for_add_users)

    await state.update_data(delete_mode=False, add_account=acc)

    await callback.message.edit_text(

        f"👤 Аккаунт-резолвер: **{acc}**\n\n"

        f"➕ **Добавление пользователей в базу**\n\n"

        f"Отправьте список, по одному на строку:\n\n"

        f"**Формат:**\n"

        f"• `@username Имя` — добавить с именем для `{{user}}`\n"

        f"• `@username` — без имени\n\n"

        f"**Пример:**\n"

        f"`@marina_petrova Марина`\n"

        f"`@ivan_ivanov Иван`\n\n"

        f"💡 Бот проверит, что username существует, и сохранит в базу.",

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅ Назад", callback_data="db_menu")]

        ])

    )


@dp.message(BotStates.waiting_for_add_users)

async def process_add_or_delete(message: Message, state: FSMContext):

    user_id = message.from_user.id

    data = await state.get_data()

    if data.get("delete_mode"):

        ids_raw = [tok.strip() for tok in (message.text or "").replace(",", " ").split() if tok.strip()]

        ids = [int(t) for t in ids_raw if t.lstrip("-").isdigit()]

        ids = list(set(ids))

        if not ids:

            await message.answer("❌ Не нашёл id. Пришлите числа.")

            return

        placeholders = ",".join("?" * len(ids))

        async with aiosqlite.connect(DB_PATH) as db:

            cur = await db.execute(

                f"DELETE FROM parsed_users WHERE owner_id=? AND user_id IN ({placeholders})",

                (user_id, *ids)

            )

            deleted = cur.rowcount or 0

            await db.commit()

        await state.clear()

        await message.answer(

            f"🗑 Удалено из базы: **{deleted}** из {len(ids)}.",

            reply_markup=get_db_keyboard()

        )

        return


    config = await load_config(user_id)

    account_name = data.get("add_account") or (config.get("session_names", [None])[0])

    if not account_name:

        await state.clear()

        await message.answer("❌ Нет активного аккаунта. Сначала добавьте его в ‘Аккаунты / Сессии’.")

        return


    lines = [l.strip() for l in (message.text or "").split("\n") if l.strip()]

    if not lines:

        await message.answer("❌ Пустой список. Пришлите хотя бы одну строку.")

        return


    status_msg = await message.answer(f"⏳ Обрабатываю {len(lines)} строк через **{account_name}**…")

    success_msgs = []

    failed_msgs = []

    added = 0

    for line in lines:

        parsed = parse_user_line(line)

        if not parsed:

            failed_msgs.append(f"❌ {line[:60]} — невалидный формат (ожидаю `@username Имя`)")

            continue

        username, display_name = parsed

        ok, msg = await resolve_and_add_user(user_id, username, display_name, account_name)

        if ok:

            added += 1

            success_msgs.append(msg)

        else:

            failed_msgs.append(msg)

    success_text = "\n".join(success_msgs[:15]) if success_msgs else "—"

    if len(success_msgs) > 15:

        success_text += f"\n… и ещё {len(success_msgs) - 15}"

    fail_text = "\n".join(failed_msgs[:15]) if failed_msgs else "—"

    if len(failed_msgs) > 15:

        fail_text += f"\n… и ещё {len(failed_msgs) - 15}"

    await status_msg.edit_text(

        f"✅ **Добавление завершено!**\n\n"

        f"➕ Успешно: **{added}**\n"

        f"❌ Ошибок: **{len(failed_msgs)}**\n\n"

        f"**Успешно:**\n{success_text}\n\n"

        f"**Ошибки:**\n{fail_text}",

        reply_markup=get_db_keyboard()

    )

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

        f"  • `{{user}}` — **имя из базы** (то, что вы указали при добавлении)\n"

        f"  • `{{first_name}}` — имя получателя из Telegram\n"

        f"  • `{{username}}` — @username или имя\n\n"

        f"**Пример:**\n"

        f"`Здравствуйте {{user}}!`\n"

        f"Подставит: «Здравствуйте Марина!», «Здравствуйте Иван!», и т.д.\n\n"

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

    db_stats = await get_user_db_stats(user_id)

    db_state = await get_db_mailing_state(user_id)

    text = (f"📊 **Ваша статистика:**\n\n"

            f"• Тариф: **Бесплатный (без лимитов)**\n"

            f"• Всего отправлено: {config['stats']['sent_count']}\n"

            f"• Текущая задержка: {config['delays']['min']}-{config['delays']['max']} сек.\n"

            f"• Загружено чатов: {len(config['chats'])}\n"

            f"• Активных аккаунтов: {len(config.get('session_names', []))}\n\n"

            f"📥 **База получателей:**\n"

            f"• Всего: {db_stats['total']} (активных: {db_stats['active']}, в архиве: {db_stats['archived']})\n"

            f"• Рассылка по базе: **{db_state.get('status', 'stopped')}**")

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

