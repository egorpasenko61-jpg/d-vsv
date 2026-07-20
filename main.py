import asyncio
import json
import os
import random
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
import aiohttp

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

# Взаимодействие с CryptoBot API (Исправленный официальный URL)
async def create_crypto_invoice(amount: float, user_id: int):
    url = "https://pay.cryptobot.net/api/createInvoice"
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
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("ok"):
                        return res_json["result"]["pay_url"], res_json["result"]["invoice_id"]
                else:
                    print(f"CryptoBot API вернул статус {resp.status}: {await resp.text()}")
    except Exception as e:
        print(f"Ошибка создания счета CryptoBot: {e}")
    return None, None

async def check_crypto_invoice(invoice_id: int):
    url = f"https://pay.cryptobot.net/api/getInvoices?invoice_ids={invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("ok") and res_json["result"]["items"]:
                        return res_json["result"]["items"][0]["status"] == "paid"
    except Exception as e:
        print(f"Ошибка проверки счета CryptoBot: {e}")
    return False

async def init_telethon_accounts_for_user(user_id: int):
    global telethon_accounts
    config = await load_config(user_id)
    if user_id not in telethon_accounts:
        telethon_accounts[user_id] = {}
        
    for session_name in config.get("session_names", []):
        client = TelegramClient(session_name, API_ID, API_HASH)
        try:
            await client.connect()
            if await client.is_user_authorized():
                telethon_accounts[user_id][session_name] = client
        except Exception as e:
            print(f"❌ Ошибка загрузки сессии {session_name} для {user_id}: {e}")

async def init_all_existing_accounts():
    for file in os.listdir("."):
        if file.startswith("config_") and file.endswith(".json"):
            try:
                user_id = int(file.split("_")[1].split(".")[0])
                await init_telethon_accounts_for_user(user_id)
            except ValueError:
                continue

async def mailing_worker_for_user(user_id: int):
    while True:
        config = await load_config(user_id)
        user_clients = telethon_accounts.get(user_id, {})
        
        if config["status"] == "started":
            if not config["chats"] or not user_clients:
                await asyncio.sleep(5)
                continue
            
            for chat in config["chats"]:
                config = await load_config(user_id)
                if config["status"] != "started": 
                    break
                
                # Лимиты для обычных пользователей
                if not config["is_premium"] and config["daily_sent"] >= FREE_LIMIT:
                    config["status"] = "stopped"
                    await save_config(user_id, config)
                    print(f"🛑 Юзер {user_id} исчерпал дневной лимит.")
                    break
                
                for acc_name, client in list(user_clients.items()):
                    try:
                        if not client.is_connected():
                            await client.connect()
                        
                        msg_text = config["scenarios"].get(acc_name, "Привет!")
                        
                        # Если нет премиума — лепим вотермарку
                        if not config["is_premium"]:
                            msg_text += WATERMARK
                            
                        target = int(chat) if str(chat).lstrip('-').isdigit() else chat
                        await client.send_message(target, msg_text)
                        
                        config["stats"]["sent_count"] += 1
                        config["daily_sent"] += 1
                        await save_config(user_id, config)
                        print(f"[{user_id}][{acc_name}] Отправлено в {chat} ({config['daily_sent']}/{FREE_LIMIT if not config['is_premium'] else '∞'})")
                        
                    except FloodWaitError as e:
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        try: await client.disconnect() 
                        except: pass
                    
                    delay = random.randint(config["delays"]["min"], config["delays"]["max"])
                    await asyncio.sleep(delay)
        else:
            break
        await asyncio.sleep(3)

active_workers = {}

def start_user_worker(user_id: int):
    if user_id not in active_workers or active_workers[user_id].done():
        active_workers[user_id] = asyncio.create_task(mailing_worker_for_user(user_id))

def get_main_keyboard(status="stopped", is_premium=False):
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
        [InlineKeyboardButton(text="📝 Сценарий", callback_data="scenario"), InlineKeyboardButton(text="💬 Чаты", callback_data="chats_menu")],
        [InlineKeyboardButton(text="⏱ Настроить задержки", callback_data="set_delay"), InlineKeyboardButton(text="📈 Статистика", callback_data="stats_view")],
        [InlineKeyboardButton(text="🔑 Аккаунты / Сессии", callback_data="accounts_manage")]
    ]
    
    if not is_premium:
        buttons.append([InlineKeyboardButton(text="⭐ Купить Премиум ($1.5)", callback_data="buy_premium")])
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)

bot_storage = MemoryStorage()
dp = Dispatcher(storage=bot_storage)

@dp.callback_query(F.data == "ignore_click")
async def cb_ignore(callback: CallbackQuery):
    await callback.answer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    config = await load_config(user_id)
    await message.answer("📋 **Главное меню панели управления рассылками:**", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))

@dp.callback_query(F.data == "back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    await callback.message.edit_text("📋 **Главное меню панели управления рассылками:**", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))

@dp.callback_query(F.data == "buy_premium")
async def cb_buy_premium(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    if config["is_premium"]:
        await callback.answer("⭐ У вас уже есть Премиум!", show_alert=True)
        return
        
    pay_url, invoice_id = await create_crypto_invoice(SUB_PRICE_USD, user_id)
    if not pay_url:
        await callback.answer("❌ Ошибка платежной системы. Проверьте настройки приложения в Crypto Pay.", show_alert=True)
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
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
    await callback.answer("🚀 Рассылка успешно запущена!", show_alert=False)
    start_user_worker(user_id)

@dp.callback_query(F.data == "pause")
async def cb_pause(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    config["status"] = "paused"
    await save_config(user_id, config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
    await callback.answer("⏸ Поставлено на паузу.", show_alert=False)

@dp.callback_query(F.data == "stop")
async def cb_stop(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    config["status"] = "stopped"
    await save_config(user_id, config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
    await callback.answer("⏹ Рассылка остановлена.", show_alert=False)

@dp.callback_query(F.data == "reset_status")
async def cb_reset_status(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    config["status"] = "stopped"
    await save_config(user_id, config)
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
    await callback.message.edit_reply_markup(reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
    await message.answer(f"✅ Текст для **{acc_name}** обновлен!", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
    await state.clear()

@dp.callback_query(F.data == "chats_menu")
async def cb_chats(callback: CallbackQuery):
    user_id = callback.from_user.id
    config = await load_config(user_id)
    chats_list = "\n".join([f"• {c}" for c in config["chats"]]) if config["chats"] else "Список пуст"
    text = f"💬 **Чаты ({len(config['chats'])}):**\n\n{chats_list}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Импортировать", callback_data="import_chats")], [InlineKeyboardButton(text="⬅ Назад", callback_data="back_to_menu")]])
    await callback.message.edit_text(text, reply_markup=kb)

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
    await message.answer(f"✅ Всего: {len(config['chats'])} чатов загружено.", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
    await message.answer(f"✅ Задержки обновлены: **{min_delay}-{max_delay} сек.**", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
        
        await message.answer("✅ Аккаунт добавлен!", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
        await message.answer("✅ Аккаунт добавлен!", reply_markup=get_main_keyboard(config["status"], config["is_premium"]))
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
    print("Инициализация существующих аккаунтов пользователей...")
    await init_all_existing_accounts()
    
    for user_id in telethon_accounts.keys():
        config = await load_config(user_id)
        if config.get("status") == "started":
            start_user_worker(user_id)
            
    print("Бот успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
