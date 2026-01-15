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

# Конфигурация бота
BOT_TOKEN = "8554779199:AAGdjsBeBYOsIkqMffCvDHvbElNWWul4VUA"
ADMIN_ID = 469946528  # Telegram ID администратора

# Постоянные клавиатуры
def get_user_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="📖 Меню мероприятий")
    keyboard.adjust(1)
    return keyboard.as_markup(resize_keyboard=True)

def get_admin_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="➕ Добавить мероприятие")
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
                phone TEXT NOT NULL,
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

    def _add_missing_columns(self):
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

    def get_targeted_users(self, gender=None):
        query = "SELECT * FROM users WHERE 1=1"
        params = []
        if gender:
            query += " AND gender = ?"
            params.append(gender)
        self.cursor.execute(query, params)
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
            logging.error(f"Ошибка удаления мероприятия: {e}")
            return False

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
            logging.error(f"Ошибка получения заявок по мероприятию: {e}")
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
                return None, "Мероприятие не найдено.", None
            applications = self.get_applications_by_event(event_id)
            if not applications:
                return None, "Нет заявок на это мероприятие.", None
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
            logging.error(f"Ошибка экспорта заявок мероприятия в Excel: {e}")
            return None, str(e), None

    def close(self):
        self.conn.close()


class RegistrationStates(StatesGroup):
    waiting_for_consent = State()
    waiting_for_phone = State()
    waiting_for_full_name = State()
    waiting_for_gender = State()
    waiting_for_birth_date = State()
    waiting_for_events_choice = State()

class AdminStates(StatesGroup):
    waiting_for_event_title = State()
    waiting_for_event_description = State()
    waiting_for_event_photo = State()
    waiting_for_rocket_description = State()
    waiting_for_rocket_photo = State()
    waiting_for_broadcast_message = State()
    waiting_for_broadcast_photo = State()
    waiting_for_broadcast_target = State()
    waiting_for_stats_action = State()
    waiting_for_event_action = State()
    waiting_for_resend_choice = State()
    waiting_for_resend_title = State()
    waiting_for_resend_description = State()
    waiting_for_resend_photo = State()


class EventBot:
    def __init__(self):
        self.bot = Bot(token=BOT_TOKEN)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.db = Database()
        self._register_handlers()
        self.dp.include_router(self.router)

    def _register_handlers(self):
        self.router.message.register(self.cmd_start, Command(commands=['start']))
        self.router.message.register(self.cmd_menu, Command(commands=['menu']))
        self.router.message.register(self.cmd_admin, Command(commands=['admin']))

        self.router.message.register(self.handle_menu_button, F.text == "📖 Меню мероприятий")
        self.router.message.register(self.handle_admin_event_button, F.text == "➕ Добавить мероприятие")
        self.router.message.register(self.handle_admin_rocket_button, F.text == "🚀 Вступить в ракету")
        self.router.message.register(self.handle_admin_broadcast_button, F.text == "✉️ Написать сообщение")
        self.router.message.register(self.handle_admin_stats_button, F.text == "📊 Статистика и выгрузки")
        self.router.message.register(self.handle_skip_button, F.text == "⏭️ Пропустить")

        self.router.callback_query.register(self.process_consent, F.data == 'consent_yes', StateFilter(RegistrationStates.waiting_for_consent))
        self.router.message.register(self.process_phone, F.content_type == ContentType.CONTACT, StateFilter(RegistrationStates.waiting_for_phone))
        self.router.message.register(self.process_phone_manual, StateFilter(RegistrationStates.waiting_for_phone))
        self.router.message.register(self.process_full_name, StateFilter(RegistrationStates.waiting_for_full_name))
        self.router.callback_query.register(self.process_gender, F.data.startswith('gender_'), StateFilter(RegistrationStates.waiting_for_gender))
        self.router.message.register(self.process_birth_date, StateFilter(RegistrationStates.waiting_for_birth_date))
        self.router.callback_query.register(self.process_events_choice, F.data.startswith('send_events_'), StateFilter(RegistrationStates.waiting_for_events_choice))

        self.router.message.register(self.process_event_title, StateFilter(AdminStates.waiting_for_event_title))
        self.router.message.register(self.process_event_description, StateFilter(AdminStates.waiting_for_event_description))
        self.router.message.register(self.process_event_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_event_photo))
        self.router.message.register(self.skip_event_photo, StateFilter(AdminStates.waiting_for_event_photo))

        self.router.message.register(self.process_rocket_description, StateFilter(AdminStates.waiting_for_rocket_description))
        self.router.message.register(self.process_rocket_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_rocket_photo))
        self.router.message.register(self.skip_rocket_photo, StateFilter(AdminStates.waiting_for_rocket_photo))

        self.router.message.register(self.process_broadcast_message, StateFilter(AdminStates.waiting_for_broadcast_message))
        self.router.message.register(self.process_broadcast_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_broadcast_photo))
        self.router.message.register(self.skip_broadcast_photo, StateFilter(AdminStates.waiting_for_broadcast_photo))
        self.router.callback_query.register(self.process_broadcast_target, F.data.startswith('target_'), StateFilter(AdminStates.waiting_for_broadcast_target))

        self.router.callback_query.register(self.process_stats_action, F.data.startswith('stats_'), StateFilter(AdminStates.waiting_for_stats_action))
        self.router.callback_query.register(self.process_event_action, F.data.startswith('event_'), StateFilter(AdminStates.waiting_for_event_action))
        self.router.callback_query.register(self.process_event_resend_choice, F.data.startswith('event_resend_'))

        self.router.callback_query.register(self.handle_resend_choice, F.data.in_({"resend_same", "resend_new"}), StateFilter(AdminStates.waiting_for_resend_choice))
        self.router.message.register(self.process_resend_title, StateFilter(AdminStates.waiting_for_resend_title))
        self.router.message.register(self.process_resend_description, StateFilter(AdminStates.waiting_for_resend_description))
        self.router.message.register(self.process_resend_photo, F.content_type == ContentType.PHOTO, StateFilter(AdminStates.waiting_for_resend_photo))
        self.router.message.register(self.skip_resend_photo, StateFilter(AdminStates.waiting_for_resend_photo))

        self.router.callback_query.register(self.handle_apply, F.data.startswith('apply_'))
        self.router.callback_query.register(self.handle_rocket_apply, F.data == 'rocket_apply')

    async def cmd_start(self, message: types.Message, state: FSMContext):
        await state.clear()
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        if user:
            keyboard = get_user_keyboard() if user_id != ADMIN_ID else get_admin_keyboard()
            await message.answer("Вы уже зарегистрированы. Используйте кнопки ниже для навигации.", reply_markup=keyboard)
            return

        await message.answer(
            "👋🏻Приветствую! Я бот сообщества предпринимателей РАКЕТА🚀\n"
            "Я помогу вам быть в курсе всех анонсов событий и путешествий. Для начала работы мне нужно получить ваши контактные данные."
        )
        await asyncio.sleep(2)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="✅ Согласен", callback_data="consent_yes")
        await message.answer(
            "Нажимая «✅ Согласен», вы соглашаетесь на обработку персональных данных для получения информации о сообществе и его событиях!",
            reply_markup=keyboard.as_markup()
        )
        await state.set_state(RegistrationStates.waiting_for_consent)

    async def process_consent(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        keyboard = ReplyKeyboardBuilder()
        keyboard.button(text="📱 Отправить контакт", request_contact=True)
        await callback_query.message.answer(
            "Пожалуйста, отправьте ваш номер телефона:",
            reply_markup=keyboard.as_markup(resize_keyboard=True, one_time_keyboard=True)
        )
        await state.set_state(RegistrationStates.waiting_for_phone)

    async def process_phone(self, message: types.Message, state: FSMContext):
        phone = message.contact.phone_number
        await state.update_data(phone=phone)
        await message.answer("Введите ваше полное имя (ФИО):")
        await state.set_state(RegistrationStates.waiting_for_full_name)

    async def process_phone_manual(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        phone = message.text.strip()
        clean_phone = re.sub(r'[^\d+]', '', phone)
        if not re.match(r'^(\+7|8)?\d{10}$', clean_phone):
            await message.answer("Некорректный номер телефона. Попробуйте снова или отправьте контакт через кнопку.\nПример: +79991234567")
            return
        if clean_phone.startswith('8'):
            clean_phone = '+7' + clean_phone[1:]
        elif not clean_phone.startswith('+7'):
            clean_phone = '+7' + clean_phone
        await state.update_data(phone=clean_phone)
        await message.answer("Введите ваше полное имя (ФИО):")
        await state.set_state(RegistrationStates.waiting_for_full_name)

    async def process_full_name(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        full_name = message.text.strip()
        if not re.match(r'^[А-Яа-яЁё\s\-]{5,50}$', full_name):
            await message.answer("Пожалуйста, введите корректное ФИО (только русские буквы, пробелы и дефисы, минимум 5 символов).")
            return
        name_parts = full_name.split()
        user_name = name_parts[1] if len(name_parts) > 1 else name_parts[0]
        await state.update_data(full_name=full_name, user_name=user_name)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Мужской 👨", callback_data="gender_male")
        keyboard.button(text="Женский 👩", callback_data="gender_female")
        keyboard.adjust(2)
        await message.answer("Выберите ваш пол:", reply_markup=keyboard.as_markup())
        await state.set_state(RegistrationStates.waiting_for_gender)

    async def process_gender(self, callback_query: types.CallbackQuery, state: FSMContext):
        gender = 'male' if callback_query.data == 'gender_male' else 'female'
        await state.update_data(gender=gender)
        await callback_query.answer()
        await callback_query.message.answer("Введите вашу дату рождения в формате ДД.ММ.ГГГГ (например, 01.01.1990):")
        await state.set_state(RegistrationStates.waiting_for_birth_date)

    async def process_birth_date(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        birth_date_str = message.text.strip()
        try:
            birth_date = datetime.strptime(birth_date_str, '%d.%m.%Y')
            today = datetime.now()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            if age < 18 or age > 100:
                await message.answer("Пожалуйста, введите корректную дату рождения (возраст должен быть от 18 до 100 лет).")
                return
            birth_date_db = birth_date.strftime('%Y-%m-%d')
            await state.update_data(birth_date=birth_date_db)

            data = await state.get_data()
            user_id = message.from_user.id
            username = message.from_user.username
            full_name = data['full_name']
            phone = data['phone']
            gender = data['gender']
            user_name = data.get('user_name', full_name.split()[0])

            self.db.add_user(user_id, full_name, phone, username, gender, birth_date_db)

            await message.answer(f"✅ Спасибо, {user_name}! Теперь Вы будете получать анонсы событий и путешествий🚀")

            await state.set_state(RegistrationStates.waiting_for_events_choice)
            await state.update_data(user_name=user_name)

            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="Да", callback_data="send_events_yes")
            keyboard.button(text="Нет", callback_data="send_events_no")
            keyboard.adjust(2)
            await message.answer(f"{user_name}, отправить вам актуальный список событий?", reply_markup=keyboard.as_markup())

        except ValueError:
            await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ДД.ММ.ГГГГ (например, 01.01.1990).")

    async def process_events_choice(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        data = await state.get_data()
        user_name = data.get('user_name', 'Пользователь')
        choice = callback_query.data
        user_id = callback_query.from_user.id
        await state.clear()

        if choice == "send_events_yes":
            events = self.db.get_all_events()
            if events:
                await callback_query.message.answer("🎉 Текущие события:\nВыберите событие для участия:")
                for event in events:
                    event_id, title, description, photo_id, created_at = event
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
                    caption = f"🔹 {title}\n{description}"
                    if photo_id:
                        try:
                            await self.bot.send_photo(callback_query.message.chat.id, photo_id, caption=caption, reply_markup=keyboard.as_markup())
                        except Exception as e:
                            logging.error(f"Ошибка отправки фото мероприятия: {e}")
                            await callback_query.message.answer(caption, reply_markup=keyboard.as_markup())
                    else:
                        await callback_query.message.answer(caption, reply_markup=keyboard.as_markup())
            else:
                await callback_query.message.answer("Пока что нет актуальных мероприятий. Следите за сообщениями!")

        keyboard = get_user_keyboard() if user_id != ADMIN_ID else get_admin_keyboard()
        await callback_query.message.answer("Используйте меню ниже для навигации:", reply_markup=keyboard)

    async def cmd_menu(self, message: types.Message):
        await self.show_menu(message)

    async def handle_menu_button(self, message: types.Message):
        await self.show_menu(message)

    async def show_menu(self, message: types.Message):
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь через /start")
            return
        events = self.db.get_all_events()
        if not events:
            keyboard = get_user_keyboard() if message.from_user.id != ADMIN_ID else get_admin_keyboard()
            await message.answer(
                "Пока что нет актуальных мероприятий. Следите за сообщениями!",
                reply_markup=keyboard
            )
            return
        await message.answer("🎉 Текущие события:\nВыберите событие для участия:")
        for event in events[:3]:
            event_id, title, description, photo_id, created_at = event
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
            caption = f"🔹 {title}\n{description}"
            if photo_id:
                try:
                    await self.bot.send_photo(message.chat.id, photo_id, caption=caption, reply_markup=keyboard.as_markup())
                except Exception as e:
                    logging.error(f"Ошибка отправки фото мероприятия: {e}")
                    await message.answer(caption, reply_markup=keyboard.as_markup())
            else:
                await message.answer(caption, reply_markup=keyboard.as_markup())
        keyboard = get_user_keyboard() if message.from_user.id != ADMIN_ID else get_admin_keyboard()
        await message.answer("Используйте меню ниже для навигации:", reply_markup=keyboard)

    async def cmd_admin(self, message: types.Message, state: FSMContext):
        if message.from_user.id != ADMIN_ID:
            await message.answer("У вас нет прав администратора")
            return
        await state.clear()
        await message.answer("Админ-панель:", reply_markup=get_admin_keyboard())

    async def handle_admin_stats_button(self, message: types.Message, state: FSMContext):
        if message.from_user.id != ADMIN_ID:
            return
        await state.clear()
        total_users = len(self.db.get_all_users())
        total_events = len(self.db.get_all_events())
        total_applications = self.db.cursor.execute('SELECT COUNT(*) FROM applications').fetchone()[0]
        rocket_applications = self.db.cursor.execute('SELECT COUNT(*) FROM applications WHERE rocket_application = 1').fetchone()[0]
        stats_message = (
            f"📊 Статистика:\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"🎉 Всего мероприятий: {total_events}\n"
            f"📝 Всего заявок: {total_applications}\n"
            f"🚀 Заявок в ракету: {rocket_applications}"
        )
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="📥 Выгрузить всех пользователей", callback_data="stats_export_users")
        keyboard.button(text="📥 Выгрузить все заявки", callback_data="stats_export_applications")
        keyboard.button(text="📋 Показать мероприятия", callback_data="stats_show_events")
        keyboard.adjust(1)
        await message.answer(stats_message, reply_markup=keyboard.as_markup())
        await state.set_state(AdminStates.waiting_for_stats_action)

    async def process_stats_action(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        action = callback_query.data
        if action == "stats_export_users":
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
            await state.clear()
            return

        elif action == "stats_export_applications":
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
            await state.clear()
            return

        elif action == "stats_show_events":
            events = self.db.get_all_events()
            if not events:
                await callback_query.message.answer("Пока нет мероприятий")
                await state.clear()
                return
            response = "📋 Список мероприятий:\n"
            keyboard = InlineKeyboardBuilder()
            for event in events:
                event_id, title, description, photo_id, created_at = event
                response += f"• {title} (ID: {event_id})\n"
                keyboard.button(text=f"📥 {title}", callback_data=f"event_export_{event_id}")
                keyboard.button(text=f"🗑️ {title}", callback_data=f"event_delete_{event_id}")
                keyboard.button(text=f"📤 {title}", callback_data=f"event_resend_{event_id}")
            keyboard.adjust(2)
            await callback_query.message.answer(response, reply_markup=keyboard.as_markup())
            await state.set_state(AdminStates.waiting_for_event_action)
            return

        await callback_query.message.answer("Неизвестное действие. Пожалуйста, выберите опцию снова.")
        await state.clear()

    async def process_event_action(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        action_data = callback_query.data.split('_')
        if len(action_data) < 3:
            await callback_query.message.answer("Некорректные данные для обработки действия.")
            await state.clear()
            return
        action = action_data[1]
        try:
            event_id = int(action_data[2])
        except ValueError:
            await callback_query.message.answer("Некорректный ID мероприятия.")
            await state.clear()
            return

        if action == "export":
            result = self.db.export_event_applications_to_excel(event_id)
            buffer, error, event_title = result
            if buffer:
                document = BufferedInputFile(
                    buffer.getvalue(),
                    filename=f"event_{event_id}_applications.xlsx"
                )
                await self.bot.send_document(
                    chat_id=callback_query.from_user.id,
                    document=document,
                    caption=f"📊 Выгрузка заявок на мероприятие: {event_title}"
                )
            else:
                await callback_query.message.answer(f"❌ Ошибка экспорта: {error or 'Нет данных'}")
        elif action == "delete":
            if self.db.delete_event(event_id):
                await callback_query.message.answer(f"✅ Мероприятие успешно удалено")
            else:
                await callback_query.message.answer("❌ Ошибка при удалении мероприятия")
        await state.clear()

    async def process_event_resend_choice(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        try:
            event_id = int(callback_query.data.split('_')[2])
        except (IndexError, ValueError):
            await callback_query.message.answer("❌ Некорректный ID мероприятия.")
            return

        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("❌ Мероприятие не найдено.")
            return

        await state.update_data(event_id=event_id)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔄 Повторить то же сообщение", callback_data="resend_same")
        keyboard.button(text="✏️ Новый текст", callback_data="resend_new")
        keyboard.adjust(1)
        await callback_query.message.answer(
            f"Выберите действие для рассылки мероприятия:\n«{event[1]}»",
            reply_markup=keyboard.as_markup()
        )
        await state.set_state(AdminStates.waiting_for_resend_choice)

    async def handle_resend_choice(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        choice = callback_query.data
        data = await state.get_data()
        event_id = data['event_id']
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.message.answer("❌ Мероприятие не найдено.")
            await state.clear()
            return

        if choice == "resend_same":
            title, description, photo_id = event[1], event[2], event[3]
            await state.update_data(
                broadcast_type='event',
                content=description,
                photo_id=photo_id,
                event_id=event_id
            )
            keyboard = self._create_target_selection_keyboard("event")
            await callback_query.message.answer("Выберите аудиторию для повторной рассылки:", reply_markup=keyboard.as_markup())
            await state.set_state(AdminStates.waiting_for_broadcast_target)

        elif choice == "resend_new":
            await callback_query.message.answer("Введите новое название мероприятия:")
            await state.set_state(AdminStates.waiting_for_resend_title)

    async def process_resend_title(self, message: types.Message, state: FSMContext):
        await state.update_data(resend_title=message.text.strip())
        await message.answer("Введите новое описание мероприятия:")
        await state.set_state(AdminStates.waiting_for_resend_description)

    async def process_resend_description(self, message: types.Message, state: FSMContext):
        await state.update_data(resend_description=message.text.strip())
        await message.answer("Отправьте новое фото или нажмите 'Пропустить':", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_resend_photo)

    async def process_resend_photo(self, message: types.Message, state: FSMContext):
        if message.content_type == ContentType.PHOTO:
            photo_id = message.photo[-1].file_id
            await state.update_data(photo_id=photo_id)
        else:
            await state.update_data(photo_id=None)
        await self._finalize_resend_data(message, state)

    async def skip_resend_photo(self, message: types.Message, state: FSMContext):
        if message.text and "пропустить" in message.text.lower():
            await state.update_data(photo_id=None)
            await self._finalize_resend_data(message, state)
        else:
            await message.answer("Отправьте фото или нажмите 'Пропустить':", reply_markup=get_skip_keyboard())

    async def _finalize_resend_data(self, message, state):
        data = await state.get_data()
        event_id = data['event_id']
        description = data.get('resend_description')
        photo_id = data.get('photo_id')

        await state.update_data(
            broadcast_type='event',
            content=description,
            photo_id=photo_id,
            event_id=event_id
        )

        keyboard = self._create_target_selection_keyboard("event")
        await message.answer("Выберите аудиторию для рассылки:", reply_markup=keyboard.as_markup())
        await state.set_state(AdminStates.waiting_for_broadcast_target)

    def _create_target_selection_keyboard(self, context):
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Всем пользователям 👥", callback_data=f"target_all_{context}")
        keyboard.button(text="Только женщинам 👩", callback_data=f"target_female_{context}")
        keyboard.button(text="Только мужчинам 👨", callback_data=f"target_male_{context}")
        keyboard.adjust(1, 2)
        return keyboard

    async def handle_admin_event_button(self, message: types.Message, state: FSMContext):
        if message.from_user.id != ADMIN_ID:
            return
        await message.answer("Введите название мероприятия:")
        await state.set_state(AdminStates.waiting_for_event_title)

    async def handle_admin_rocket_button(self, message: types.Message, state: FSMContext):
        if message.from_user.id != ADMIN_ID:
            return
        await message.answer("Введите описание для вступления в ракету:")
        await state.set_state(AdminStates.waiting_for_rocket_description)

    async def handle_admin_broadcast_button(self, message: types.Message, state: FSMContext):
        if message.from_user.id != ADMIN_ID:
            return
        await message.answer("Введите текст сообщения для рассылки:")
        await state.set_state(AdminStates.waiting_for_broadcast_message)

    async def handle_skip_button(self, message: types.Message, state: FSMContext):
        current_state = await state.get_state()
        if current_state == AdminStates.waiting_for_event_photo.state:
            await state.update_data(photo_id=None)
            await self._save_and_send_event(message, state)
        elif current_state == AdminStates.waiting_for_rocket_photo.state:
            await state.update_data(photo_id=None)
            await self._send_rocket_broadcast(message, state)
        elif current_state == AdminStates.waiting_for_broadcast_photo.state:
            await state.update_data(photo_id=None)
            await self._select_broadcast_target(message, state)
        elif current_state == AdminStates.waiting_for_resend_photo.state:
            await state.update_data(photo_id=None)
            await self._finalize_resend_data(message, state)
        else:
            await message.answer("Кнопка 'Пропустить' не активна в текущем состоянии")

    async def process_event_title(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        await state.update_data(title=message.text.strip())
        await message.answer("Введите описание мероприятия:")
        await state.set_state(AdminStates.waiting_for_event_description)

    async def process_event_description(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        await state.update_data(description=message.text.strip())
        await message.answer("Отправьте фото для мероприятия или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_event_photo)

    async def process_event_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id
        await state.update_data(photo_id=photo_id)
        await self._save_and_send_event(message, state)

    async def skip_event_photo(self, message: types.Message, state: FSMContext):
        if message.text and message.text.lower() in ['пропустить', 'skip', 'нет']:
            await state.update_data(photo_id=None)
            await self._save_and_send_event(message, state)
        else:
            await message.answer("Отправьте фото или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())

    async def _save_and_send_event(self, message, state):
        data = await state.get_data()
        title = data['title']
        description = data['description']
        photo_id = data.get('photo_id')
        event_id = self.db.add_event(title, description, photo_id)
        keyboard = self._create_target_selection_keyboard("event")
        await message.answer("Выберите кому отправить мероприятие:", reply_markup=keyboard.as_markup())
        await state.update_data(event_id=event_id, broadcast_type='event')
        await state.set_state(AdminStates.waiting_for_broadcast_target)

    async def process_rocket_description(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        await state.update_data(description=message.text.strip())
        await message.answer("Отправьте фото для описания или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_rocket_photo)

    async def process_rocket_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id
        await state.update_data(photo_id=photo_id)
        await self._send_rocket_broadcast(message, state)

    async def skip_rocket_photo(self, message: types.Message, state: FSMContext):
        if message.text and message.text.lower() in ['пропустить', 'skip', 'нет']:
            await state.update_data(photo_id=None)
            await self._send_rocket_broadcast(message, state)
        else:
            await message.answer("Отправьте фото или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())

    async def _send_rocket_broadcast(self, message, state):
        data = await state.get_data()
        description = data['description']
        photo_id = data.get('photo_id')
        keyboard = self._create_target_selection_keyboard("rocket")
        await message.answer("Выберите кому отправить информацию о вступлении в ракету:", reply_markup=keyboard.as_markup())
        await state.update_data(broadcast_type='rocket', content=description, photo_id=photo_id)
        await state.set_state(AdminStates.waiting_for_broadcast_target)

    async def process_broadcast_message(self, message: types.Message, state: FSMContext):
        if not message.text:
            await message.answer("Пожалуйста, введите текст. Попробуйте еще раз.")
            return
        await state.update_data(content=message.text.strip())
        await message.answer("Отправьте фото для рассылки или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())
        await state.set_state(AdminStates.waiting_for_broadcast_photo)

    async def process_broadcast_photo(self, message: types.Message, state: FSMContext):
        photo_id = message.photo[-1].file_id
        await state.update_data(photo_id=photo_id)
        await self._select_broadcast_target(message, state)

    async def skip_broadcast_photo(self, message: types.Message, state: FSMContext):
        if message.text and message.text.lower() in ['пропустить', 'skip', 'нет']:
            await state.update_data(photo_id=None)
            await self._select_broadcast_target(message, state)
        else:
            await message.answer("Отправьте фото или нажмите кнопку 'Пропустить' ниже:", reply_markup=get_skip_keyboard())

    async def _select_broadcast_target(self, message, state):
        keyboard = self._create_target_selection_keyboard("broadcast")
        await message.answer("Выберите кому отправить сообщение:", reply_markup=keyboard.as_markup())
        await state.update_data(broadcast_type='broadcast')
        await state.set_state(AdminStates.waiting_for_broadcast_target)

    async def process_broadcast_target(self, callback_query: types.CallbackQuery, state: FSMContext):
        await callback_query.answer()
        data = callback_query.data  # <-- ЭТО КЛЮЧ!

        state_data = await state.get_data()
        broadcast_type = state_data.get('broadcast_type', 'broadcast')
        content = state_data.get('content')
        photo_id = state_data.get('photo_id')
        event_id = state_data.get('event_id')

        gender = None
        if "target_all" in data:
            users = self.db.get_all_users()
        elif "target_female" in data:
            gender = 'female'
            users = self.db.get_targeted_users(gender=gender)
        elif "target_male" in data:
            gender = 'male'
            users = self.db.get_targeted_users(gender=gender)
        else:
            users = self.db.get_all_users()

        success_count = 0
        failed_count = 0
        for user in users:
            user_id = user[0]
            try:
                if broadcast_type == "rocket":
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="🚀 Хочу вступить в ракету!", callback_data="rocket_apply")
                    if photo_id:
                        await self.bot.send_photo(user_id, photo_id, caption=content, reply_markup=keyboard.as_markup())
                    else:
                        await self.bot.send_message(user_id, content, reply_markup=keyboard.as_markup())
                elif broadcast_type == "event" and event_id:
                    event = self.db.get_event(event_id)
                    if not event:
                        continue
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="Хочу участвовать", callback_data=f"apply_{event_id}")
                    caption = f"🎉 Новое мероприятие:\n{event[1]}\n{event[2]}"
                    if photo_id:
                        await self.bot.send_photo(user_id, photo_id, caption=caption, reply_markup=keyboard.as_markup())
                    else:
                        await self.bot.send_message(user_id, caption, reply_markup=keyboard.as_markup())
                else:
                    if photo_id:
                        await self.bot.send_photo(user_id, photo_id, caption=content)
                    else:
                        await self.bot.send_message(user_id, content)
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
            report += "Тип рассылки: Новое мероприятие"
        else:
            report += "Тип рассылки: Обычное сообщение"

        if gender:
            report += "\nЦелевая аудитория:"
            if gender == 'female':
                report += "\n- Пол: Женский"
            elif gender == 'male':
                report += "\n- Пол: Мужской"

        keyboard = get_admin_keyboard()
        await callback_query.message.answer(report, reply_markup=keyboard)
        await state.clear()

    async def handle_apply(self, callback_query: types.CallbackQuery):
        event_id = int(callback_query.data.split('_')[1])
        user_id = callback_query.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await callback_query.answer("Вы не зарегистрированы. Пожалуйста, начните с /start.", show_alert=True)
            return
        event = self.db.get_event(event_id)
        if not event:
            await callback_query.answer("Мероприятие не найдено.", show_alert=True)
            return
        application_id = self.db.add_application(user_id, event_id)
        if not application_id:
            await callback_query.answer("Вы уже подавали заявку на это мероприятие!", show_alert=True)
            return
        username = callback_query.from_user.username or "не указан"
        telegram_link = f"@{username}" if username else "не указан"
        admin_message = (
            f"✅ Новая заявка на событие «{event[1]}»\n"
            f"👤 «Ракетчик»: {user[1]}\n"
            f"📞 Телефон: {user[2]}\n"
            f"💬 Telegram: {telegram_link}\n"
            f"🆔 ID заявки: {application_id}"
        )
        try:
            await self.bot.send_message(ADMIN_ID, admin_message)
        except Exception as e:
            logging.error(f"Ошибка отправки админу: {e}")
            await callback_query.answer("Произошла ошибка при отправке заявки. Попробуйте еще раз.", show_alert=True)
            return
        await callback_query.answer()
        await callback_query.message.answer(f"✅ Отлично! Менеджер свяжется с вами в течение 15 минут.\nОжидайте звонка на номер: {user[2]}")

    async def handle_rocket_apply(self, callback_query: types.CallbackQuery):
        user_id = callback_query.from_user.id
        user = self.db.get_user(user_id)
        if not user:
            await callback_query.answer("Вы не зарегистрированы. Пожалуйста, начните с /start.", show_alert=True)
            return
        application_id = self.db.add_application(user_id, rocket_application=True)
        if not application_id:
            await callback_query.answer("Вы уже подавали заявку на вступление в ракету!", show_alert=True)
            return
        username = callback_query.from_user.username or "не указан"
        telegram_link = f"@{username}" if username else "не указан"
        gender_str = "Мужской" if user[4] == 'male' else "Женский"
        admin_message = (
            f"🚀 Заявка на вступление в ракету\n"
            f"👤 Пользователь: {user[1]}\n"
            f"⚤ Пол: {gender_str}\n"
            f"🎂 Дата рождения: {user[5]}\n"
            f"📞 Телефон: {user[2]}\n"
            f"💬 Telegram: {telegram_link}\n"
            f"🆔 ID заявки: {application_id}"
        )
        try:
            await self.bot.send_message(ADMIN_ID, admin_message)
        except Exception as e:
            logging.error(f"Ошибка отправки админу: {e}")
            await callback_query.answer("Произошла ошибка при отправке заявки. Попробуйте еще раз.", show_alert=True)
            return
        await callback_query.answer()
        await callback_query.message.answer(f"🚀 Отлично! Администратор свяжется с вами в ближайшее время.\nОжидайте звонка на номер: {user[2]}")

    def run(self):
        self.dp.run_polling(self.bot)
        self.db.close()


if __name__ == '__main__':
    bot = EventBot()
    bot.run()