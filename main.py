import os
import sqlite3
import re
import logging
import asyncio
from datetime import datetime
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.filters import Command, StateFilter
from aiogram.types import ReplyKeyboardRemove, ContentType, BufferedInputFile
import pandas as pd
from io import BytesIO
# Настройка логирования
logging.basicConfig(level=logging.INFO)
# 🔑 Конфигурация — ВСЁ В КОДЕ
BOT_TOKEN = "8215527179:AAH-Mm4-ePZEPCbh1P7B1HA_V7bV0TPsyk0"
ADMIN_IDS = [1565932131, 469946528]
def get_user_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="📖 Меню событий")
    keyboard.button(text="🚀 Вступить в ракету!")
    keyboard.adjust(1)
    return keyboard.as_markup(resize_keyboard=True)
def get_admin_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="➕ Добавить событие")
    keyboard.button(text="🚀 Вступить в ракету")
    keyboard.button(text="✉️ Написать сообщение")
    keyboard.button(text="📊 Статистика и выгрузки")
    keyboard.adjust(2, 2)
    return keyboard.as_markup(resize_keyboard=True)
def get_skip_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="⏭️ Пропустить")
    return keyboard.as_markup(resize_keyboard=True, one_time_keyboard=True)
class Database:
    def __init__(self, db_name='event_bot.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.update_schema()
    def update_schema(self):
        self._create_tables()
        self._add_missing_columns()
        self.conn.commit()
    def _create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL UNIQUE,
                username TEXT,
                gender TEXT CHECK(gender IN ('male', 'female')),
                birth_date TEXT,
                has_children BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                photo_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                event_id INTEGER,
                rocket_application BOOLEAN DEFAULT 0,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(event_id) REFERENCES events(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS rocket_info (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                title TEXT,
                description TEXT,
                photo_id TEXT
            )
        ''')
    def _add_missing_columns(self):
        try:
            self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_phone ON users(phone);")
        except Exception as e:
            logging.warning(f"Не удалось создать уникальный индекс для phone: {e}")
        self.cursor.execute("PRAGMA table_info(events)")
        columns = [col[1] for col in self.cursor.fetchall()]
        if 'photo_id' not in columns:
            logging.info("Adding photo_id column to events table")
            self.cursor.execute('ALTER TABLE events ADD COLUMN photo_id TEXT')
        self.cursor.execute("PRAGMA table_info(users)")
        user_columns = [col[1] for col in self.cursor.fetchall()]
        required_user_columns = ['gender', 'birth_date']
        for col in required_user_columns:
            if col not in user_columns:
                logging.info(f"Adding {col} column to users table")
                if col == 'gender':
                    self.cursor.execute('ALTER TABLE users ADD COLUMN gender TEXT CHECK(gender IN ("male", "female"))')
                elif col == 'birth_date':
                    self.cursor.execute('ALTER TABLE users ADD COLUMN birth_date TEXT')
        self.cursor.execute("PRAGMA table_info(applications)")
        app_columns = [col[1] for col in self.cursor.fetchall()]
        if 'rocket_application' not in app_columns:
            logging.info("Adding rocket_application column to applications table")
            self.cursor.execute('ALTER TABLE applications ADD COLUMN rocket_application BOOLEAN DEFAULT 0')
    def is_phone_registered(self, phone: str, exclude_user_id: int = None) -> bool:
        if exclude_user_id:
            self.cursor.execute('SELECT user_id FROM users WHERE phone = ? AND user_id != ?', (phone, exclude_user_id))
        else:
            self.cursor.execute('SELECT user_id FROM users WHERE phone = ?', (phone,))
        return self.cursor.fetchone() is not None
    def add_user(self, user_id, full_name, phone, username=None, gender=None, birth_date=None):
        self.cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
        exists = self.cursor.fetchone()
        if exists:
            self.cursor.execute('''
                UPDATE users SET
                    full_name = ?,
                    phone = ?,
                    username = ?,
                    gender = ?,
                    birth_date = ?,
                    has_children = 0
                WHERE user_id = ?
            ''', (full_name, phone, username, gender, birth_date, user_id))
        else:
            self.cursor.execute('''
                INSERT INTO users
                    (user_id, full_name, phone, username, gender, birth_date, has_children)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            ''', (user_id, full_name, phone, username, gender, birth_date))
        self.conn.commit()
    def get_user(self, user_id):
        self.cursor.execute('SELECT * FROM users WHERE user_id=?', (user_id,))
        return self.cursor.fetchone()
    def get_all_users(self):
        self.cursor.execute('SELECT * FROM users')
        return self.cursor.fetchall()
    def add_event(self, title, description, photo_id=None):
        self.cursor.execute('INSERT INTO events (title, description, photo_id) VALUES (?, ?, ?)', (title, description, photo_id))
        self.conn.commit()
        return self.cursor.lastrowid
    def get_event(self, event_id):
        self.cursor.execute('SELECT * FROM events WHERE id=?', (event_id,))
        return self.cursor.fetchone()
    def get_all_events(self):
        self.cursor.execute('SELECT * FROM events ORDER BY created_at DESC')
        return self.cursor.fetchall()
    def delete_event(self, event_id):
        try:
            self.cursor.execute('DELETE FROM applications WHERE event_id = ?', (event_id,))
            self.cursor.execute('DELETE FROM events WHERE id = ?', (event_id,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Ошибка удаления события: {e}")
            return False
    def update_event(self, event_id, title, description, photo_id):
        self.cursor.execute('''
            UPDATE events
            SET title = ?, description = ?, photo_id = ?
            WHERE id = ?
        ''', (title, description, photo_id, event_id))
        self.conn.commit()
    def get_rocket_info(self):
        self.cursor.execute('SELECT * FROM rocket_info LIMIT 1')
        return self.cursor.fetchone()
    def update_rocket_info(self, title=None, description=None, photo_id=None):
        current = self.get_rocket_info()
        if current:
            new_title = title if title is not None else current[1]
            new_desc = description if description is not None else current[2]
            new_photo = photo_id if photo_id is not None else current[3]
            self.cursor.execute('''
                UPDATE rocket_info SET title = ?, description = ?, photo_id = ?
                WHERE id = 1
            ''', (new_title, new_desc, new_photo))
        else:
            self.cursor.execute('''
                INSERT INTO rocket_info (id, title, description, photo_id)
                VALUES (1, ?, ?, ?)
            ''', (title, description, photo_id))
        self.conn.commit()
    def add_application(self, user_id, event_id=None, rocket_application=False):
        try:
            if rocket_application:
                self.cursor.execute('SELECT id FROM applications WHERE user_id = ? AND rocket_application = 1', (user_id,))
            elif event_id:
                self.cursor.execute('SELECT id FROM applications WHERE user_id = ? AND event_id = ?', (user_id, event_id))
            if self.cursor.fetchone():
                return None
            if rocket_application:
                self.cursor.execute('INSERT INTO applications (user_id, rocket_application) VALUES (?, ?)', (user_id, 1))
            else:
                self.cursor.execute('INSERT INTO applications (user_id, event_id) VALUES (?, ?)', (user_id, event_id))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logging.error(f"Ошибка добавления заявки: {e}")
            return None
    def get_applications_by_event(self, event_id):
        try:
            query = '''
                SELECT a.id as application_id, u.full_name, u.phone, u.username, u.gender, u.birth_date, a.applied_at
                FROM applications a
                JOIN users u ON a.user_id = u.user_id
                WHERE a.event_id = ?
                ORDER BY a.applied_at DESC
            '''
            self.cursor.execute(query, (event_id,))
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка получения заявок по событию: {e}")
            return []
    def get_rocket_applications(self):
        try:
            query = '''
                SELECT a.id as application_id, u.full_name, u.phone, u.username, u.gender, u.birth_date, a.applied_at
                FROM applications a
                JOIN users u ON a.user_id = u.user_id
                WHERE a.rocket_application = 1
                ORDER BY a.applied_at DESC
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка получения заявок на ракету: {e}")
            return []
    def export_users_to_excel(self):
        try:
            query = 'SELECT user_id, full_name, phone, username, gender, birth_date, created_at FROM users ORDER BY created_at DESC'
            df = pd.read_sql_query(query, self.conn)
            if df.empty:
                return None, "Нет данных для экспорта пользователей."
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Users')
            buffer.seek(0)
            return buffer, None
        except Exception as e:
            logging.error(f"Ошибка экспорта пользователей в Excel: {e}")
            return None, str(e)
    def export_applications_to_excel(self):
        try:
            query = '''
                SELECT
                    a.id as application_id,
                    u.full_name,
                    u.phone,
                    u.username,
                    u.gender,
                    u.birth_date,
                    e.title as event_title,
                    a.rocket_application,
                    a.applied_at
                FROM applications a
                JOIN users u ON a.user_id = u.user_id
                LEFT JOIN events e ON a.event_id = e.id
                ORDER BY a.applied_at DESC
            '''
            df = pd.read_sql_query(query, self.conn)
            if df.empty:
                return None, "Нет данных для экспорта заявок."
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Applications')
            buffer.seek(0)
            return buffer, None
        except Exception as e:
            logging.error(f"Ошибка экспорта заявок в Excel: {e}")
            return None, str(e)
    def export_event_applications_to_excel(self, event_id):
        try:
            event = self.get_event(event_id)
            if not event:
                return None, "Событие не найдено.", None
            applications = self.get_applications_by_event(event_id)
            if not applications:
                return None, "Нет заявок на это событие.", None
            df = pd.DataFrame(applications, columns=[
                'application_id', 'full_name', 'phone', 'username',
                'gender', 'birth_date', 'applied_at'
            ])
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Event Applications')
            buffer.seek(0)
            return buffer, None, event[1]
        except Exception as e:
            logging.error(f"Ошибка экспорта заявок события в Excel: {e}")
            return None, str(e), None
    def close(self):
        self.conn.close()
class RegistrationStates(StatesGroup):
    waiting_for_consent = State()
    waiting_for_phone = State()
    waiting_for_full_name = State()
    waiting_for_events_choice = State()
class AdminStates(StatesGroup):
    waiting_for_event_title = State()
    waiting_for_event_description = State()
    waiting_for_event_photo = State()
    waiting_for_rocket_title = State()
    waiting_for_rocket_description = State()
    waiting_for_rocket_photo = State()
    waiting_for_edit_rocket_title = State()
    waiting_for_edit_rocket_description = State()
    waiting_for_edit_rocket_photo = State()
    waiting_for_broadcast_message = State()
    waiting_for_broadcast_photo = State()
    waiting_for_broadcast_target = State()
    waiting_for_custom_event_broadcast = State()
    waiting_for_custom_event_photo = State()
    waiting_for_edit_event_id = State()
    waiting_for_edit_title = State()
    waiting_for_edit_description = State()
    waiting_for_edit_photo = State()
    waiting_for_delete_confirmation = State()
class EventBot:
    CAPTION_MAX_LENGTH = 1024
    def __init__(self):
        self.bot = Bot(token=BOT_TOKEN)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.db = Database()
        self._register_handlers()
        self.dp.include_router(self.router)
    def is_admin(self, user_id: int) -> bool:
        return user_id in ADMIN_IDS
    def _register_handlers(self):
        self.router.message.register(self.cmd_start, Command(commands=['start']))
        self.router.message.register(self.cmd_menu, Command(commands=['menu']))
        self.router.message.register(self.cmd_admin, Command(commands=['admin']))
        self.router.message.register(self.handle_menu_button, F.text == "📖 Меню событий")
        self.router.message.register(self.handle_rocket_menu_button, F.text == "🚀 Вступить в ракету!")
        self.router.message.register(self.handle_admin_event_button, F.text == "➕ Добавить событие")
        self.router.message.register(self.handle_admin_rocket_button, F.text == "🚀 Вступить в ракету")
        self.router.message.register(self.handle_admin_broadcast_button, F.text == "✉️ Написать сообщение")
        self.router.message.register(self.handle_admin_stats_button, F.text == "📊 Статистика и выгрузки")
        self.router.message.register(self.handle_skip_button, F.text == "⏭️ Пропустить")
        # Регистрация
        self.router.callback_query.register(self.process_consent, F.data == 'consent_yes', StateFilter(RegistrationStates.waiting_for_consent))
        self.router.message.register(self.process_phone, F.content_type == ContentType.CONTACT, StateFilter(RegistrationStates.waiting_for_phone))
        self.router.message.register(self.process_phone_manual, StateFilter(RegistrationStates.waiting_for_phone))
        self.router.message.register(self.process_full_name, StateFilter(RegistrationStates.waiting_for_full_name))
        self.router.callback_query.register(self.process_events_choice, F.data.startswith('send_events_'), StateFilter(RegistrationStates.waiting_for_events_choice))
        # Админка — события
        self.router.message.register(self.process_event_title, StateFilter(AdminStates.waiting_for_event_title))
        self.router.message.register(self.process_event_description, StateFilter(AdminStates.waiting_for_event_description))
        self.router.message.register(self.process_event_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_event_photo))
        self.router.message.register(self.skip_event_photo, StateFilter(AdminStates.waiting_for_event_photo))
        # Админка — ракета (создание)
        self.router.message.register(self.process_rocket_title, StateFilter(AdminStates.waiting_for_rocket_title))
        self.router.message.register(self.process_rocket_description, StateFilter(AdminStates.waiting_for_rocket_description))
        self.router.message.register(self.process_rocket_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_rocket_photo))
        self.router.message.register(self.skip_rocket_photo, StateFilter(AdminStates.waiting_for_rocket_photo))
        # Админка — ракета (редактирование)
        self.router.message.register(self.process_edit_rocket_title, StateFilter(AdminStates.waiting_for_edit_rocket_title))
        self.router.message.register(self.process_edit_rocket_description, StateFilter(AdminStates.waiting_for_edit_rocket_description))
        self.router.message.register(self.process_edit_rocket_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_edit_rocket_photo))
        self.router.message.register(self.skip_edit_rocket_photo, StateFilter(AdminStates.waiting_for_edit_rocket_photo))
        # Админка — рассылка
        self.router.message.register(self.process_broadcast_message, StateFilter(AdminStates.waiting_for_broadcast_message))
        self.router.message.register(self.process_broadcast_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_broadcast_photo))
        self.router.message.register(self.skip_broadcast_photo, StateFilter(AdminStates.waiting_for_broadcast_photo))
        self.router.callback_query.register(self.process_broadcast_target, F.data.startswith('target_'), StateFilter(AdminStates.waiting_for_broadcast_target))
        # Статистика
        self.router.callback_query.register(self.handle_stats_export_users, F.data == "stats_export_users")
        self.router.callback_query.register(self.handle_stats_export_applications, F.data == "stats_export_applications")
        self.router.callback_query.register(self.handle_stats_show_events, F.data == "stats_show_events")
        self.router.callback_query.register(self.handle_event_export, F.data.startswith('event_export_'))
        self.router.callback_query.register(self.handle_event_delete, F.data.startswith('event_delete_'))
        # Рассылка по событию
        self.router.callback_query.register(self.start_custom_broadcast_for_event, F.data.startswith('event_custom_broadcast_'))
        self.router.message.register(self.process_custom_broadcast_text, StateFilter(AdminStates.waiting_for_custom_event_broadcast))
        self.router.message.register(self.process_custom_broadcast_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_custom_event_photo))
        self.router.message.register(self.skip_custom_broadcast_photo, StateFilter(AdminStates.waiting_for_custom_event_photo))
        # Редактирование события
        self.router.callback_query.register(self.process_event_edit_choice, F.data.startswith('event_edit_'))
        self.router.message.register(self.process_edit_title, StateFilter(AdminStates.waiting_for_edit_title))
        self.router.message.register(self.process_edit_description, StateFilter(AdminStates.waiting_for_edit_description))
        self.router.message.register(self.process_edit_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_edit_photo))
        self.router.message.register(self.skip_edit_photo, StateFilter(AdminStates.waiting_for_edit_photo))
        # Удаление — подтверждение
        self.router.callback_query.register(self.confirm_delete_event, F.data.startswith('confirm_delete_'), StateFilter(AdminStates.waiting_for_delete_confirmation))
        self.router.callback_query.register(self.cancel_delete_event, F.data.startswith('cancel_delete_'), StateFilter(AdminStates.waiting_for_delete_confirmation))
        # Заявки
        self.router.callback_query.register(self.handle_apply, F.data.startswith('apply_'))
        self.router.callback_query.register(self.handle_rocket_apply, F.data == 'rocket_apply')
        # Просмотр события
        self.router.callback_query.register(self.handle_view_event, F.data.startswith("view_event_"))
        self.router.callback_query.register(self.handle_admin_view_event, F.data.startswith("admin_view_event_"))
        self.router.callback_query.register(self.handle_event_resend_all, F.data.startswith("event_resend_all_"))
    async def _send_long_message_with_photo_and_button(self, chat_id, text, photo_id, reply_markup):
        if len(text) <= self.CAPTION_MAX_LENGTH:
            await self.bot.send_photo(chat_id, photo_id, caption=text, reply_markup=reply_markup)
        else:
            await self.bot.send_message(chat_id, text)
            await self.bot.send_photo(chat_id, photo_id, caption="", reply_markup=reply_markup)
    async def cmd_start(self, message: types.Message, state: FSMContext):
        await state.clear()
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        if user:
            keyboard = get_user_keyboard() if not self.is_admin(user_id) else get_admin_keyboard()
            await message.answer("Ты уже зарегистрирован. Используй кнопки ниже для навигации.", reply_markup=keyboard)
            return
        await message.answer(
            "👋🏻Привет! Я бот сообщества предпринимателей РАКЕТА🚀\n"
            "Я помогу тебе быть в курсе всех анонсов наших событий и путешествий😎\n"
            "Для начала работы мне нужно получить твои контактные данные🤝"
        )
        await asyncio.sleep(2)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="✅ Согласен", callback_data="consent_yes")
        await message.answer(
            "Нажимая «✅ Согласен», ты соглашаешься на обработку персональных данных для получения информации о сообществе и его событиях!",
            reply_markup=keyboard.as_markup()
        )
        await state.set_state(RegistrationStates.waiting_for_consent)
    async def process_consent(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        keyboard = ReplyKeyboardBuilder()
        keyboard.button(text="📱 Отправить контакт", request_contact=True)
        await callback_query.message.answer(
            "Пожалуйста, отправь свой номер телефона:",
            reply_markup=keyboard.as_markup(resize_keyboard=True, one_time_keyboard=True)
        )
        await state.set_state(RegistrationStates.waiting_for_phone)
    async def process_phone(self, message: types.Message, state: FSMContext):
        phone = message.contact.phone_number
        clean_phone = re.sub(r'[^\d+]', '', phone)
        if clean_phone.startswith('8'):
            clean_phone = '+7' + clean_phone[1:]
        elif not clean_phone.startswith('+7'):
            clean_phone = '+7' + clean_phone
        if self.db.is_phone_registered(clean_phone):
            await message.answer("❌ Этот номер телефона уже зарегистрирован. Отправь другой номер:")
            return
        await state.update_data(phone=clean_phone)
        await message.answer("Напиши своё имя:")
        await state.set_state(RegistrationStates.waiting_for_full_name)
    async def process_phone_manual(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введи текст. Попробуй ещё раз.")
            return
        phone = message.text.strip()
        clean_phone = re.sub(r'[^\d+]', '', phone)
        if not re.match(r'^(\+7|8)?\d{10}$', clean_phone):
            await message.answer("Некорректный номер телефона. Попробуй снова или отправь контакт через кнопку.\nПример: +79991234567")
            return
        if clean_phone.startswith('8'):
            clean_phone = '+7' + clean_phone[1:]
        elif not clean_phone.startswith('+7'):
            clean_phone = '+7' + clean_phone
        if self.db.is_phone_registered(clean_phone):
            await message.answer("❌ Этот номер телефона уже зарегистрирован. Отправь другой номер:")
            return
        await state.update_data(phone=clean_phone)
        await message.answer("Напиши своё имя (можно указать до 3 слов, например: Анна или Иван Петров):")
        await state.set_state(RegistrationStates.waiting_for_full_name)
    async def process_full_name(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Имя не может быть пустым. Попробуй снова.")
            return
        name_input = message.text.strip()
        words = name_input.split()
        if len(words) == 0:
            await message.answer("Имя не может быть пустым. Попробуй снова.")
            return
        display_name = " ".join(words[:3])
        if not re.match(r'^[А-Яа-яЁё\s\-]{3,}$', display_name):
            await message.answer("Имя должно содержать минимум 3 символа и состоять только из русских букв, пробелов и дефисов.")
            return
        data = await state.get_data()
        phone = data['phone']
        user_id = message.from_user.id
        username = message.from_user.username
        self.db.add_user(user_id, display_name, phone, username)
        await message.answer(f"✅ Спасибо, {display_name}! Теперь ты будешь получать анонсы событий и путешествий🚀")
        await state.update_data(user_name=display_name)
        await state.set_state(RegistrationStates.waiting_for_events_choice)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Да", callback_data="send_events_yes")
        keyboard.button(text="Нет", callback_data="send_events_no")
        keyboard.adjust(2)
        await message.answer(f"{display_name}, отправить тебе актуальный список событий?", reply_markup=keyboard.as_markup())
    async def process_events_choice(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        user_id = callback_query.from_user.id
        choice = callback_query.data
        await state.clear()
        if choice == "send_events_yes":
            await self.show_menu(callback_query.message)
            # Отправляем клавиатуру после показа меню
            keyboard = get_user_keyboard() if not self.is_admin(user_id) else get_admin_keyboard()
            await callback_query.message.answer("Используй меню ниже для навигации:", reply_markup=keyboard)
        else:
            keyboard = get_user_keyboard() if not self.is_admin(user_id) else get_admin_keyboard()
            await callback_query.message.answer("Используй меню ниже для навигации:", reply_markup=keyboard)
    async def cmd_menu(self, message: types.Message):
        await self.show_menu(message)
    async def handle_menu_button(self, message: types.Message):
        await self.show_menu(message)
    async def show_menu(self, message: types.Message):
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await message.answer("Сначала зарегистрируйся через /start")
            return
        events = self.db.get_all_events()
        if not events:
            keyboard = get_user_keyboard() if not self.is_admin(user_id) else get_admin_keyboard()
            await message.answer(
                "извините пока что нет ожидайте следите за обновлением",
                reply_markup=keyboard
            )
            return
        builder = InlineKeyboardBuilder()
        for event in events:
            event_id = event[0]
            title = event[1]
            builder.button(text=title, callback_data=f"view_event_{event_id}")
        builder.adjust(1)
        await message.answer("🎉 Выбери событие:", reply_markup=builder.as_markup())
    async def handle_view_event(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split("_")[2])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("Событие не найдено.")
            return
        _, title, description, photo_id, _ = event
        caption = f"{title}\n{description}\n"
        builder = InlineKeyboardBuilder()
        builder.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
        if photo_id:
            try:
                await self._send_long_message_with_photo_and_button(
                    callback_query.message.chat.id,
                    caption,
                    photo_id,
                    builder.as_markup()
                )
            except Exception as e:
                logging.error(f"Ошибка отправки фото события: {e}")
                await callback_query.message.answer(caption, reply_markup=builder.as_markup())
        else:
            await callback_query.message.answer(caption, reply_markup=builder.as_markup())
    async def handle_rocket_menu_button(self, message: types.Message):
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await message.answer("Сначала зарегистрируйся через /start")
            return
        rocket = self.db.get_rocket_info()
        if not rocket or not rocket[1]:
            await message.answer("Информация о вступлении в РАКЕТУ пока не готова. Следи за обновлениями!")
            return
        _, title, description, photo_id = rocket
        caption = f"{title}\n{description}"
        builder = InlineKeyboardBuilder()
        builder.button(text="Хочу вступить в ракету!", callback_data="rocket_apply")
        if photo_id:
            try:
                await self._send_long_message_with_photo_and_button(
                    message.chat.id, caption, photo_id, builder.as_markup()
                )
            except Exception as e:
                logging.error(f"Ошибка отправки фото ракеты: {e}")
                await message.answer(caption, reply_markup=builder.as_markup())
        else:
            await message.answer(caption, reply_markup=builder.as_markup())
    async def cmd_admin(self, message: types.Message, state: FSMContext):
        if not self.is_admin(message.from_user.id):
            await message.answer("У тебя нет прав администратора")
            return
        await state.clear()
        await message.answer("Админ-панель:", reply_markup=get_admin_keyboard())
    async def handle_admin_event_button(self, message: types.Message, state: FSMContext):
        if not self.is_admin(message.from_user.id):
            return
        await message.answer("Введи название события:")
        await state.set_state(AdminStates.waiting_for_event_title)
    async def handle_admin_rocket_button(self, message: types.Message, state: FSMContext):
        if not self.is_admin(message.from_user.id):
            return
        rocket = self.db.get_rocket_info()
        if rocket:
            await message.answer("Ракета уже настроена. Хочешь изменить название? Введи новое или 'Пропустить':", reply_markup=get_skip_keyboard())
            await state.set_state(AdminStates.waiting_for_edit_rocket_title)
        else:
            await message.answer("Введи название для вступления в ракету:")
            await state.set_state(AdminStates.waiting_for_rocket_title)
    async def handle_admin_broadcast_button(self, message: types.Message, state: FSMContext):
        if not self.is_admin(message.from_user.id):
            return
        await message.answer("Введи текст сообщения для рассылки:")
        await state.set_state(AdminStates.waiting_for_broadcast_message)
    async def handle_admin_stats_button(self, message: types.Message, state: FSMContext):
        if not self.is_admin(message.from_user.id):
            return
        total_users = len(self.db.get_all_users())
        total_events = len(self.db.get_all_events())
        total_applications = self.db.cursor.execute('SELECT COUNT(*) FROM applications').fetchone()[0]
        rocket_applications = self.db.cursor.execute('SELECT COUNT(*) FROM applications WHERE rocket_application = 1').fetchone()[0]
        stats_message = (
            f"📊 Статистика:\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"🎉 Всего событий: {total_events}\n"
            f"📝 Всего заявок: {total_applications}\n"
            f"🚀 Заявок в ракету: {rocket_applications}"
        )
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="📥 Выгрузить всех пользователей", callback_data="stats_export_users")
        keyboard.button(text="📥 Выгрузить все заявки", callback_data="stats_export_applications")
        keyboard.button(text="📋 Показать события", callback_data="stats_show_events")
        keyboard.adjust(1)
        await message.answer(stats_message, reply_markup=keyboard.as_markup())
    # 🔥 ИСПРАВЛЕННЫЙ ОБРАБОТЧИК ПРОПУСТИТЬ
    async def handle_skip_button(self, message: types.Message, state: FSMContext):
        current_state = await state.get_state()
        # События
        if current_state == AdminStates.waiting_for_event_photo.state:
            await state.update_data(photo_id=None)
            await self._save_and_send_event(message, state)
        # Ракета — создание
        elif current_state == AdminStates.waiting_for_rocket_photo.state:
            await state.update_data(rocket_photo_id=None)
            await self._finalize_rocket_creation(message, state)
        # Ракета — редактирование
        elif current_state == AdminStates.waiting_for_edit_rocket_title.state:
            await state.update_data(edit_rocket_title=None)
            await message.answer("Введи новое описание или 'Пропустить':", reply_markup=get_skip_keyboard())
            await state.set_state(AdminStates.waiting_for_edit_rocket_description)
        elif current_state == AdminStates.waiting_for_edit_rocket_description.state:
            await state.update_data(edit_rocket_description=None)
            await message.answer("Отправь новое фото или 'Пропустить':", reply_markup=get_skip_keyboard())
            await state.set_state(AdminStates.waiting_for_edit_rocket_photo)
        elif current_state == AdminStates.waiting_for_edit_rocket_photo.state:
            await state.update_data(edit_rocket_photo_id=None)
            await self._finalize_rocket_edit(message, state)
        # Обычная рассылка
        elif current_state == AdminStates.waiting_for_broadcast_photo.state:
            await state.update_data(photo_id=None)
            await self._select_broadcast_target(message, state)
        # Кастомная рассылка события
        elif current_state == AdminStates.waiting_for_custom_event_photo.state:
            await state.update_data(custom_photo_id=None)
            await self._send_custom_event_broadcast(message, state)
        # Редактирование события — название
        elif current_state == AdminStates.waiting_for_edit_title.state:
            await state.update_data(edit_title=None)
            await message.answer("Введи новое описание или 'Пропустить':", reply_markup=get_skip_keyboard())
            await state.set_state(AdminStates.waiting_for_edit_description)
        elif current_state == AdminStates.waiting_for_edit_description.state:
            await state.update_data(edit_description=None)
            await message.answer("Отправь новое фото или 'Пропустить':", reply_markup=get_skip_keyboard())
            await state.set_state(AdminStates.waiting_for_edit_photo)
        elif current_state == AdminStates.waiting_for_edit_photo.state:
            await state.update_data(edit_photo_id=None)
            await self._apply_event_edit(message, state)
        else:
            await message.answer("Кнопка 'Пропустить' не активна в текущем состоянии.")
    # === СОЗДАНИЕ РАКЕТЫ ===
    async def process_rocket_title(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Введи текст.")
            return
        await state.update_data(rocket_title=message.text.strip())
        await message.answer("Введи описание:")
        await state.set_state(AdminStates.waiting_for_rocket_description)
    async def process_rocket_description(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Введи текст.")
            return
        await state.update_data(rocket_description=message.text.strip())
        await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_rocket_photo)
    async def process_rocket_photo(self, message: types.Message, state: FSMContext):
        if message.content_type == ContentType.PHOTO:
            photo_id = message.photo[-1].file_id
            await state.update_data(rocket_photo_id=photo_id)
        else:
            await state.update_data(rocket_photo_id=None)
        await self._finalize_rocket_creation(message, state)
    async def skip_rocket_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(rocket_photo_id=None)
            await self._finalize_rocket_creation(message, state)
        else:
            await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
    async def _finalize_rocket_creation(self, message, state):
        data = await state.get_data()
        self.db.update_rocket_info(
            title=data['rocket_title'],
            description=data['rocket_description'],
            photo_id=data.get('rocket_photo_id')
        )
        await message.answer("✅ Ракета успешно настроена!", reply_markup=get_admin_keyboard())
        await state.clear()
    # === РЕДАКТИРОВАНИЕ РАКЕТЫ ===
    async def process_edit_rocket_title(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            title = None
        else:
            title = message.text.strip() if message.text else None
        await state.update_data(edit_rocket_title=title)
        await message.answer("Введи новое описание или 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_edit_rocket_description)
    async def process_edit_rocket_description(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            desc = None
        else:
            desc = message.text.strip() if message.text else None
        await state.update_data(edit_rocket_description=desc)
        await message.answer("Отправь новое фото или 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_edit_rocket_photo)
    async def process_edit_rocket_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id if message.content_type == ContentType.PHOTO else None
        await state.update_data(edit_rocket_photo_id=photo_id)
        await self._finalize_rocket_edit(message, state)
    async def skip_edit_rocket_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(edit_rocket_photo_id=None)
            await self._finalize_rocket_edit(message, state)
        else:
            await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
    async def _finalize_rocket_edit(self, message, state):
        data = await state.get_data()
        self.db.update_rocket_info(
            title=data.get('edit_rocket_title'),
            description=data.get('edit_rocket_description'),
            photo_id=data.get('edit_rocket_photo_id')
        )
        await message.answer("✅ Ракета обновлена!", reply_markup=get_admin_keyboard())
        await state.clear()
    # === ОСТАЛЬНЫЕ МЕТОДЫ ===
    async def process_event_title(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введи текст. Попробуй ещё раз.")
            return
        await state.update_data(title=message.text.strip())
        await message.answer("Введи описание события:")
        await state.set_state(AdminStates.waiting_for_event_description)
    async def process_event_description(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введи текст. Попробуй ещё раз.")
            return
        await state.update_data(description=message.text.strip())
        await message.answer("Отправь фото для события или нажми кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_event_photo)
    async def process_event_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id
        await state.update_data(photo_id=photo_id)
        await self._save_and_send_event(message, state)
    async def skip_event_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(photo_id=None)
            await self._save_and_send_event(message, state)
        else:
            await message.answer("Отправь фото или нажми кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
    async def _save_and_send_event(self, message, state):
        data = await state.get_data()
        title = data['title']
        description = data['description']
        photo_id = data.get('photo_id')
        event_id = self.db.add_event(title, description, photo_id)
        keyboard = self._create_target_selection_keyboard("event")
        await message.answer("Выбери кому отправить событие:", reply_markup=keyboard)
        await state.update_data(event_id=event_id, broadcast_type='event')
        await state.set_state(AdminStates.waiting_for_broadcast_target)
    async def process_broadcast_message(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введи текст. Попробуй ещё раз.")
            return
        await state.update_data(content=message.text.strip())
        await message.answer("Отправь фото для рассылки или нажми кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_broadcast_photo)
    async def process_broadcast_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id
        await state.update_data(photo_id=photo_id)
        await self._select_broadcast_target(message, state)
    async def skip_broadcast_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(photo_id=None)
            await self._select_broadcast_target(message, state)
        else:
            await message.answer("Отправь фото или нажми кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
    async def _select_broadcast_target(self, message, state):
        keyboard = self._create_target_selection_keyboard("broadcast")
        await message.answer("Выбери кому отправить сообщение:", reply_markup=keyboard)
        await state.update_data(broadcast_type='broadcast')
        await state.set_state(AdminStates.waiting_for_broadcast_target)
    def _create_target_selection_keyboard(self, context):
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Всем пользователям 👥", callback_data=f"target_all_{context}")
        return keyboard.as_markup()
    async def process_broadcast_target(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        data = callback_query.data
        state_data = await state.get_data()
        broadcast_type = state_data.get('broadcast_type', 'broadcast')
        content = state_data.get('content')
        photo_id = state_data.get('photo_id')
        event_id = state_data.get('event_id')
        users = self.db.get_all_users()
        success_count = 0
        failed_count = 0
        for user in users:
            user_id = user[0]
            try:
                if broadcast_type == "rocket":
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="🚀 Хочу вступить в ракету!", callback_data="rocket_apply")
                    caption = f"{content}\n"
                    if photo_id:
                        await self._send_long_message_with_photo_and_button(user_id, caption, photo_id, keyboard.as_markup())
                    else:
                        await self.bot.send_message(user_id, caption, reply_markup=keyboard.as_markup())
                elif broadcast_type == "event" and event_id:
                    event = self.db.get_event(event_id)
                    if not event:
                        continue
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
                    caption = f"🎉 Новое событие:\n{event[1]}\n{event[2]}\n"
                    if photo_id:
                        await self._send_long_message_with_photo_and_button(user_id, caption, photo_id, keyboard.as_markup())
                    else:
                        await self.bot.send_message(user_id, caption, reply_markup=keyboard.as_markup())
                else:
                    caption = f"{content}\n"
                    if photo_id:
                        await self.bot.send_photo(user_id, photo_id, caption=caption)
                    else:
                        await self.bot.send_message(user_id, caption)
                success_count += 1
            except Exception as e:
                logging.error(f"Ошибка отправки пользователю {user_id}: {e}")
                failed_count += 1
        report = (
            f"✅ Рассылка завершена!\n"
            f"Отправлено: {success_count} из {len(users)}\n"
            f"Неудачно: {failed_count}\n"
        )
        if broadcast_type == "rocket":
            report += "Тип рассылки: Вступить в ракету"
        elif broadcast_type == "event":
            report += "Тип рассылки: Новое событие"
        else:
            report += "Тип рассылки: Обычное сообщение"
        keyboard = get_admin_keyboard()
        await callback_query.message.answer(report, reply_markup=keyboard)
        await state.clear()
    async def handle_stats_export_users(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        buffer, error = self.db.export_users_to_excel()
        if buffer:
            document = BufferedInputFile(buffer.getvalue(), filename="users.xlsx")
            await self.bot.send_document(
                chat_id=callback_query.from_user.id,
                document=document,
                caption="📊 Выгрузка всех пользователей"
            )
        else:
            await callback_query.message.answer(f"❌ Ошибка при экспорте пользователей: {error or 'Нет данных'}")
    async def handle_stats_export_applications(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        buffer, error = self.db.export_applications_to_excel()
        if buffer:
            document = BufferedInputFile(buffer.getvalue(), filename="applications.xlsx")
            await self.bot.send_document(
                chat_id=callback_query.from_user.id,
                document=document,
                caption="📊 Выгрузка всех заявок"
            )
        else:
            await callback_query.message.answer(f"❌ Ошибка при экспорте заявок: {error or 'Нет данных'}")
    async def handle_stats_show_events(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        events = self.db.get_all_events()
        if not events:
            await callback_query.message.answer("Пока нет событий")
            return
        builder = InlineKeyboardBuilder()
        for event in events:
            event_id = event[0]
            title = event[1]
            builder.button(text=title, callback_data=f"admin_view_event_{event_id}")
        builder.adjust(1)
        await callback_query.message.answer("📋 Выбери событие:", reply_markup=builder.as_markup())
    # 🔥 ИСПРАВЛЕНО: показываем только название
    async def handle_admin_view_event(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split("_")[3])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("Событие не найдено.")
            return
        title = event[1]  # Только название
        caption = f"Управление событием:\n<b>{title}</b>"
        builder = InlineKeyboardBuilder()
        builder.button(text="📥 Выгрузить заявки", callback_data=f"event_export_{event_id}")
        builder.button(text="🗑️ Удалить", callback_data=f"event_delete_{event_id}")
        builder.button(text="✏️ Редактировать", callback_data=f"event_edit_{event_id}")
        builder.button(text="📤 Рассылка", callback_data=f"event_custom_broadcast_{event_id}")
        builder.button(text="🔄 Повторная рассылка", callback_data=f"event_resend_all_{event_id}")
        builder.button(text="🔙 Назад к списку", callback_data="stats_show_events")
        builder.adjust(2, 2, 2)
        # Отправляем ТОЛЬКО текст (без фото!)
        await callback_query.message.answer(caption, reply_markup=builder.as_markup(), parse_mode="HTML")
    async def handle_event_export(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[2])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        buffer, error, event_title = self.db.export_event_applications_to_excel(event_id)
        if buffer:
            filename = f"{event_title.replace(' ', '_')}_applications.xlsx"
            document = BufferedInputFile(buffer.getvalue(), filename=filename)
            await self.bot.send_document(
                chat_id=callback_query.from_user.id,
                document=document,
                caption=f"📊 Заявки на событие «{event_title}»"
            )
        else:
            await callback_query.message.answer(f"❌ Ошибка: {error or 'Нет данных'}")
    async def handle_event_delete(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[2])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("❌ Событие не найдено.")
            return
        await state.update_data(delete_event_id=event_id)
        await state.set_state(AdminStates.waiting_for_delete_confirmation)
        confirm_keyboard = InlineKeyboardBuilder()
        confirm_keyboard.button(text="✅ Да, удалить", callback_data=f"confirm_delete_{event_id}")
        confirm_keyboard.button(text="❌ Отмена", callback_data=f"cancel_delete_{event_id}")
        confirm_keyboard.adjust(2)
        await callback_query.message.answer(
            f"Ты уверен, что хочешь удалить событие?\n«{event[1]}»",
            reply_markup=confirm_keyboard.as_markup()
        )
    async def start_custom_broadcast_for_event(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.rsplit('_', 1)[1])
        except (ValueError, IndexError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("❌ Событие не найдено.")
            return
        await state.update_data(broadcast_event_id=event_id, event_title=event[1])
        await callback_query.message.answer("Введи описание для рассылки этого события:")
        await state.set_state(AdminStates.waiting_for_custom_event_broadcast)
    async def process_custom_broadcast_text(self, message: types.Message, state: FSMContext):
        await state.update_data(custom_broadcast_text=message.text.strip())
        await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_custom_event_photo)
    async def process_custom_broadcast_photo(self, message: types.Message, state: FSMContext):
        if message.content_type == ContentType.PHOTO:
            photo_id = message.photo[-1].file_id
            await state.update_data(custom_photo_id=photo_id)
        else:
            await state.update_data(custom_photo_id=None)
        await self._send_custom_event_broadcast(message, state)
    async def skip_custom_broadcast_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(custom_photo_id=None)
            await self._send_custom_event_broadcast(message, state)
        else:
            await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
    async def _send_custom_event_broadcast(self, message: types.Message, state: FSMContext):
        data = await state.get_data()
        event_id = data['broadcast_event_id']
        event_title = data['event_title']
        custom_text = data['custom_broadcast_text']
        photo_id = data.get('custom_photo_id')
        full_text = f"{event_title}\n{custom_text}\n"
        users = self.db.get_all_users()
        success, fail = 0, 0
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
        for user in users:
            try:
                if photo_id:
                    await self._send_long_message_with_photo_and_button(user[0], full_text, photo_id, keyboard.as_markup())
                else:
                    await self.bot.send_message(user[0], full_text, reply_markup=keyboard.as_markup())
                success += 1
            except Exception as e:
                logging.error(f"Ошибка отправки {user[0]}: {e}")
                fail += 1
        await message.answer(f"✅ Рассылка отправлена!\nУспешно: {success}, Ошибок: {fail}", reply_markup=get_admin_keyboard())
        await state.clear()
    async def handle_event_resend_all(self, callback_query: types.CallbackQuery):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[3])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("Событие не найдено.")
            return
        _, title, description, photo_id, _ = event
        caption = f"{title}\n{description}\n"
        users = self.db.get_all_users()
        success, fail = 0, 0
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
        for user in users:
            try:
                if photo_id:
                    await self._send_long_message_with_photo_and_button(user[0], caption, photo_id, keyboard.as_markup())
                else:
                    await self.bot.send_message(user[0], caption, reply_markup=keyboard.as_markup())
                success += 1
            except Exception as e:
                logging.error(f"Ошибка отправки {user[0]}: {e}")
                fail += 1
        await callback_query.message.answer(
            f"✅ Повторная рассылка завершена!\nУспешно: {success}, Ошибок: {fail}"
        )
    async def process_event_edit_choice(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[2])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID события.")
            return
        await state.update_data(editing_event_id=event_id)
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("❌ Событие не найдено.")
            return
        await callback_query.message.answer(f"Редактирование события:\n«{event[1]}»\nВведи новое название или 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_edit_title)
    async def process_edit_title(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            title = None
        else:
            title = message.text.strip() if message.text else None
        await state.update_data(edit_title=title)
        await message.answer("Введи новое описание или 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_edit_description)
    async def process_edit_description(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            description = None
        else:
            description = message.text.strip() if message.text else None
        await state.update_data(edit_description=description)
        await message.answer("Отправь новое фото или 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_edit_photo)
    async def process_edit_photo(self, message: types.Message, state: FSMContext):
        if message.content_type == ContentType.PHOTO:
            photo_id = message.photo[-1].file_id
            await state.update_data(edit_photo_id=photo_id)
        else:
            await state.update_data(edit_photo_id=None)
        await self._apply_event_edit(message, state)
    async def skip_edit_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(edit_photo_id=None)
            await self._apply_event_edit(message, state)
        else:
            await message.answer("Отправь фото или нажми 'Пропустить':", reply_markup=get_skip_keyboard())
    async def _apply_event_edit(self, message: types.Message, state: FSMContext):
        data = await state.get_data()
        event_id = data['editing_event_id']
        new_title = data.get('edit_title')
        new_description = data.get('edit_description')
        new_photo_id = data.get('edit_photo_id')
        current = self.db.get_event(event_id)
        if not current:
            await message.answer("❌ Событие не найдено.")
            await state.clear()
            return
        title = new_title if new_title is not None else current[1]
        description = new_description if new_description is not None else current[2]
        photo_id = new_photo_id if new_photo_id is not None else current[3]
        self.db.update_event(event_id, title, description, photo_id)
        await message.answer(f"✅ Событие успешно обновлено!\nНазвание: {title}")
        await state.clear()
        await message.answer("Админ-панель:", reply_markup=get_admin_keyboard())
    async def confirm_delete_event(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[2])
        except:
            await callback_query.message.answer("Ошибка ID.")
            await state.clear()
            return
        if self.db.delete_event(event_id):
            await callback_query.message.answer("✅ Событие удалено.")
        else:
            await callback_query.message.answer("❌ Ошибка удаления.")
        await state.clear()
        await self.handle_admin_stats_button(callback_query.message, state)
    async def cancel_delete_event(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        await state.clear()
        await self.handle_admin_stats_button(callback_query.message, state)
    async def handle_apply(self, callback_query: types.CallbackQuery):
        event_id = int(callback_query.data.split('_')[1])
        user_id = callback_query.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await callback_query.answer("Ты не зарегистрирован. Пожалуйста, начни с /start.", show_alert=True)
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.answer("Событие не найдено.", show_alert=True)
            return
        application_id = self.db.add_application(user_id, event_id)
        if not application_id:
            await callback_query.answer("Ты уже подавал заявку на это событие!", show_alert=True)
            return
        await callback_query.answer()
        await callback_query.message.answer("✅ Отлично! Наша команда свяжется с вами в ближайшее время!")
        admin_msg = (
            f"🔔 Новая заявка на событие!\n"
            f"Пользователь: {user[1]} (ID: {user[0]})\n"
            f"Телефон: {user[2]}\n"
            f"Событие: {event[1]}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(admin_id, admin_msg)
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    async def handle_rocket_apply(self, callback_query: types.CallbackQuery):
        user_id = callback_query.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await callback_query.answer("Ты не зарегистрирован. Пожалуйста, начни с /start.", show_alert=True)
            return
        application_id = self.db.add_application(user_id, rocket_application=True)
        if not application_id:
            await callback_query.answer("Ты уже подавал заявку на вступление в ракету!", show_alert=True)
            return
        await callback_query.answer()
        await callback_query.message.answer("✅ Отлично! Наша команда свяжется с вами в ближайшее время!")
        admin_msg = (
            f"🚀 Новая заявка на вступление в РАКЕТУ!\n"
            f"Пользователь: {user[1]} (ID: {user[0]})\n"
            f"Телефон: {user[2]}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(admin_id, admin_msg)
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    def run(self):
        self.dp.run_polling(self.bot)
        self.db.close()
if __name__ == '__main__':
    bot = EventBot()
    bot.run()