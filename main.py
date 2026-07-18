import asyncio
import json
import os
import random
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, Chat, Channel
from telethon.errors import FloodWaitError, UserPrivacyRestrictedError, ChatWriteForbiddenError, SessionPasswordNeededError

CONFIG_FILE = "config.json"
config_lock = asyncio.Lock()

DEFAULT_CONFIG = {
    "api_credentials": {
        "api_id": 32155028,
        "api_hash": "ec906474420c7cc518e2245d5829924a",
        "bot_token": "7860968550:AAHNx_mJHsDrohp0DV60eTy1wCdl8gKxqmE"
    },
    "delays": {"min": 10, "max": 30},
    "stats": {"sent_count": 0},
    "scenarios": {},
    "chats": [],
    "status": "stopped",
    "session_names": []
}

class BotStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_chats = State()
    waiting_for_scenario_acc = State()
    waiting_for_scenario_text = State()
    waiting_for_delay_min = State()
    waiting_for_delay_max = State()

active_auths = {}
telethon_accounts = {}

async def load_config():
    async with config_lock:
        if not os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=4)
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

async def save_config(config_data):
    async with config_lock:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

async def init_telethon_accounts():
    global telethon_accounts
    config = await load_config()
    api_id = config["api_credentials"]["api_id"]
    api_hash = config["api_credentials"]["api_hash"]
    for session_name in config.get("session_names", []):
        client = TelegramClient(session_name, api_id, api_hash)
        try:
            await client.connect()
            if await client.is_user_authorized():
                telethon_accounts[session_name] = client
                print(f"Юзербот-сессия {session_name} загружена.")
            else:
                print(f"Сессия {session_name} не авторизована.")
        except Exception as e:
            print(f"Ошибка загрузки сессии {session_name}: {e}")

async def mailing_worker():
    while True:
        config = await load_config()
        if config["status"] == "started":
            if not config["chats"] or not telethon_accounts:
                await asyncio.sleep(5)
                continue
            
            for chat in config["chats"]:
                # Проверка паузы/стопа прямо в цикле
                config = await load_config()
                if config["status"] != "started": break
                
                for acc_name, client in list(telethon_accounts.items()):
                    try:
                        # 1. Авто-переподключение
                        if not client.is_connected():
                            await client.connect()
                        
                        # 2. Отправка
                        msg_text = config["scenarios"].get(acc_name, "Привет!")
                        target = int(chat) if str(chat).lstrip('-').isdigit() else chat
                        
                        await client.send_message(target, msg_text)
                        
                        # 3. Безопасное сохранение
                        config["stats"]["sent_count"] += 1
                        await save_config(config)
                        print(f"[{acc_name}] Успешно отправлено в {chat}")
                        
                    except FloodWaitError as e:
                        print(f"⚠️ Флуд: спим {e.seconds} сек.")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        print(f"❌ Ошибка отправки с {acc_name} в {chat}: {e}")
                        # Пытаемся разорвать связь, чтобы переподключиться в след. раз
                        try: await client.disconnect() 
                        except: pass
                    
                    # Задержка
                    delay = random.randint(config["delays"]["min"], config["delays"]["max"])
                    await asyncio.sleep(delay)
        else:
            await asyncio.sleep(3)

async def fetch_account_dialogs(client):
    dialogs_dict = {}
    try:
        result = await client(GetDialogsRequest(
            offset_date=None, offset_id=0, offset_peer=InputPeerEmpty(), limit=100, hash=0
        ))
        for chat in result.chats:
            if isinstance(chat, (Chat, Channel)):
                if getattr(chat, 'left', False) or getattr(chat, 'deactivated', False): continue
                dialogs_dict[chat.id] = {"title": chat.title, "username": f"@{chat.username}" if chat.username else f"ID: {chat.id}"}
    except Exception as e:
        print(f"Ошибка получения диалогов: {e}")
    return dialogs_dict

def get_main_keyboard(status="stopped"):
    if status == "started":
        status_row = [InlineKeyboardButton(text="🟢 РАССЫЛКА ИДЕТ 🟢", callback_data="ignore_click")]
        control_row = [InlineKeyboardButton(text="⏸ Пауза", callback_data="pause"), InlineKeyboardButton(text="⏹ Стоп", callback_data="stop")]
    elif status == "paused":
        status_row = [InlineKeyboardButton(text="🟡 НА ПАУЗЕ 🟡", callback_data="ignore_click")]
        control_row = [InlineKeyboardButton(text="▶️ Продолжить", callback_data="start"), InlineKeyboardButton(text="⏹ Стоп", callback_data="stop")]
    else:
        status_row = [InlineKeyboardButton(text="🔴 ОСТАНОВЛЕНО 🔴", callback_data="ignore_click")]
        control_row = [InlineKeyboardButton(text="🚀 Запустить", callback_data="start")]

    buttons = [
        status_row, control_row,
        [InlineKeyboardButton(text="🔄 Сбросить статусы", callback_data="reset_status"), InlineKeyboardButton(text="🧹 Глубокая очистка", callback_data="deep_clean")],
        [InlineKeyboardButton(text="📝 Сценарий", callback_data="scenario"), InlineKeyboardButton(text="💬 Чаты", callback_data="chats_menu"), InlineKeyboardButton(text="📊 Статусы", callback_data="status_info")],
        [InlineKeyboardButton(text="👥 Общие чаты", callback_data="common_chats"), InlineKeyboardButton(text="🔄 Разница чатов", callback_data="diff_chats")],
        [InlineKeyboardButton(text="⏱ Настроить задержки", callback_data="set_delay"), InlineKeyboardButton(text="📈 Статистика", callback_data="stats_view"), InlineKeyboardButton(text="🗑 Очистить стату", callback_data="clean_stats")],
        [InlineKeyboardButton(text="🔑 Аккаунты / Сессии", callback_data="accounts_manage")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

bot_storage = MemoryStorage()
dp = Dispatcher(storage=bot_storage)

@dp.callback_query(F.data == "ignore_click")
async def cb_ignore(callback: CallbackQuery):
    await callback.answer("Это индикатор статуса 👆", show_alert=False)

@dp.message(Command("start"))
async def cmd_start(message: Message):
    config = await load_config()
    await message.answer("📋 **Главное меню панели управления рассылками:**", reply_markup=get_main_keyboard(config["status"]))

@dp.callback_query(F.data == "start")
async def cb_start(callback: CallbackQuery):
    config = await load_config()
    if not telethon_accounts or not config.get("session_names"):
        await callback.answer("❌ Ошибка: Нет активных аккаунтов!", show_alert=True)
        return
    if not config["chats"]:
        await callback.answer("❌ Ошибка: Список чатов пуст!", show_alert=True)
        return

    config["status"] = "started"
    await save_config(config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"]))
    await callback.answer("🚀 Рассылка успешно запущена!", show_alert=False)

@dp.callback_query(F.data == "pause")
async def cb_pause(callback: CallbackQuery):
    config = await load_config()
    config["status"] = "paused"
    await save_config(config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"]))
    await callback.answer("⏸ Поставлено на паузу.", show_alert=False)

@dp.callback_query(F.data == "stop")
async def cb_stop(callback: CallbackQuery):
    config = await load_config()
    config["status"] = "stopped"
    await save_config(config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"]))
    await callback.answer("⏹ Рассылка остановлена.", show_alert=False)

@dp.callback_query(F.data == "reset_status")
async def cb_reset_status(callback: CallbackQuery):
    config = await load_config()
    config["status"] = "stopped"
    await save_config(config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"]))
    await callback.answer("🔄 Все состояния сброшены.", show_alert=True)

@dp.callback_query(F.data == "deep_clean")
async def cb_deep_clean(callback: CallbackQuery):
    config = await load_config()
    config["chats"] = []
    config["scenarios"] = {}
    config["status"] = "stopped"
    config["stats"]["sent_count"] = 0
    await save_config(config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"]))
    await callback.answer("🧹 Данные полностью очищены.", show_alert=True)

@dp.callback_query(F.data == "scenario")
async def cb_scenario(callback: CallbackQuery):
    config = await load_config()
    text = "📝 **Сценарии для аккаунтов:**\n\n"
    for acc in config.get("session_names", []):
        text += f"• **{acc}**: {config['scenarios'].get(acc, 'По умолчанию')}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✏️ Изменить", callback_data="edit_scenario")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query(F.data == "edit_scenario")
async def cb_edit_scenario(callback: CallbackQuery):
    config = await load_config()
    if not config.get("session_names"):
        await callback.answer("❌ Нет активных аккаунтов!", show_alert=True)
        return
    buttons = [[InlineKeyboardButton(text=acc, callback_data=f"sc_acc_{acc}")] for acc in config["session_names"]]
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")])
    await callback.message.answer("Выберите аккаунт:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("sc_acc_"))
async def cb_select_sc_acc(callback: CallbackQuery, state: FSMContext):
    acc_name = callback.data.replace("sc_acc_", "")
    await state.update_data(target_acc=acc_name)
    await state.set_state(BotStates.waiting_for_scenario_text)
    await callback.message.answer(f"📝 Введите новый текст для **{acc_name}**:")

@dp.message(BotStates.waiting_for_scenario_text)
async def process_sc_text(message: Message, state: FSMContext):
    data = await state.get_data()
    acc_name = data.get("target_acc")
    config = await load_config()
    config["scenarios"][acc_name] = message.text
    await save_config(config)
    await message.answer(f"✅ Текст для **{acc_name}** обновлен!", reply_markup=get_main_keyboard(config["status"]))
    await state.clear()

@dp.callback_query(F.data == "chats_menu")
async def cb_chats(callback: CallbackQuery):
    config = await load_config()
    chats_list = "\n".join([f"• {c}" for c in config["chats"]]) if config["chats"] else "Список пуст"
    text = f"💬 **Чаты ({len(config['chats'])}):**\n\n{chats_list}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Импортировать", callback_data="import_chats")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query(F.data == "import_chats")
async def cb_import_chats(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.waiting_for_chats)
    await callback.message.answer("📝 Отправьте список чатов (каждый с новой строки):")

@dp.message(BotStates.waiting_for_chats)
async def process_chats_list(message: Message, state: FSMContext):
    lines = message.text.split("\n")
    cleaned = []
    for line in lines:
        chat = line.strip()
        if not chat: continue
        cleaned.append("@" + chat.split("t.me/")[-1].replace("+", "") if "t.me/" in chat else chat)
    config = await load_config()
    config["chats"] = list(set(config["chats"] + cleaned))
    await save_config(config)
    await message.answer(f"✅ Всего: {len(config['chats'])}", reply_markup=get_main_keyboard(config["status"]))
    await state.clear()

@dp.callback_query(F.data == "accounts_manage")
async def cb_accounts_manage(callback: CallbackQuery):
    config = await load_config()
    text = "🔑 **Управление сессиями:**\n\n" + "\n".join([f"• {s}: {'✅ Активен' if s in telethon_accounts else '❌ Отключен'}" for s in config.get("session_names", [])])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить", callback_data="add_new_account")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query(F.data == "add_new_account")
async def cb_add_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.waiting_for_phone)
    await callback.message.answer("📱 Введите телефон:")

@dp.message(BotStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip().replace(" ", "")
    config = await load_config()
    api_id, api_hash = config["api_credentials"]["api_id"], config["api_credentials"]["api_hash"]
    new_session = f"user_account_{len(config.get('session_names', [])) + 1}"
    client = TelegramClient(new_session, api_id, api_hash)
    await client.connect()
    try:
        token = await client.send_code_request(phone)
        active_auths[message.from_user.id] = {"client": client, "phone": phone, "token": token, "session_name": new_session}
        await state.set_state(BotStates.waiting_for_code)
        await message.answer("📩 Введите код:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()

@dp.message(BotStates.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    code = "".join(filter(str.isdigit, message.text))
    if not code:
        await message.answer("❌ Нет цифр. Введите еще раз:")
        return
    auth_data = active_auths.get(message.from_user.id)
    if not auth_data: return
    try:
        await auth_data["client"].sign_in(auth_data["phone"], code, phone_code_hash=auth_data["token"].phone_code_hash)
        telethon_accounts[auth_data["session_name"]] = auth_data["client"]
        config = await load_config()
        config["session_names"].append(auth_data["session_name"])
        await save_config(config)
        await message.answer("✅ Аккаунт добавлен!", reply_markup=get_main_keyboard(config["status"]))
        await state.clear()
        active_auths.pop(message.from_user.id, None)
    except SessionPasswordNeededError:
        await state.set_state(BotStates.waiting_for_password)
        await message.answer("🔒 Введите пароль (2FA):")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(BotStates.waiting_for_password)
async def process_password(message: Message, state: FSMContext):
    auth_data = active_auths.get(message.from_user.id)
    try:
        await auth_data["client"].sign_in(password=message.text.strip())
        telethon_accounts[auth_data["session_name"]] = auth_data["client"]
        config = await load_config()
        config["session_names"].append(auth_data["session_name"])
        await save_config(config)
        await message.answer("✅ Аккаунт добавлен!", reply_markup=get_main_keyboard(config["status"]))
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

async def main():
    config = await load_config()
    bot = Bot(token=config["api_credentials"]["bot_token"])
    await init_telethon_accounts()
    asyncio.create_task(mailing_worker())
    print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())