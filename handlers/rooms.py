import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

# ── Константы ─────────────────────────────────────────────────────────────────

STATE_FILE     = "db/chatrooms_state.json"
IDLE_TIMEOUT   = 5 * 60   # 5 минут  — убирает из active (/who)
REMIND_TIMEOUT = 60 * 60  # 1 час    — личное напоминание о комнате

# ── Структуры данных ───────────────────────────────────────────────────────────

@dataclass
class Room:
    name: str
    password_hash: Optional[str] = None
    is_public: bool = True
    subscribers: dict[str, str] = field(default_factory=dict)
    active: dict[str, str] = field(default_factory=dict)

    def check_password(self, password: str) -> bool:
        if self.password_hash is None:
            return True
        return self.password_hash == hashlib.sha256(password.encode()).hexdigest()


class ChatRoomManager:
    PUBLIC_ROOMS = ["general", "offtopic", "tech"]

    def __init__(self):
        self.rooms: dict[str, Room] = {}
        self.user_room: dict[str, str] = {}
        self.user_nick: dict[str, str] = {}
        self.user_last_active: dict[str, float] = {}
        self.user_last_reminded: dict[str, float] = {}

        for name in self.PUBLIC_ROOMS:
            self.rooms[name] = Room(name=name, is_public=True)

        self._load_state()
        self._restore_active_users()

    # ── Сохранение состояния ──────────────────────────────────────────────────

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.user_nick          = data.get("user_nick", {})
            self.user_last_active   = data.get("user_last_active", {})
            self.user_last_reminded = data.get("user_last_reminded", {})

            for rdata in data.get("rooms", []):
                name = rdata["name"]
                if name not in self.rooms:
                    self.rooms[name] = Room(
                        name=name,
                        password_hash=rdata.get("password_hash"),
                        is_public=rdata.get("is_public", True),
                    )

            for uin, room_name in data.get("user_room", {}).items():
                if room_name in self.rooms:
                    nick = self.user_nick.get(uin, f"User{uin}")
                    self.user_room[uin] = room_name
                    self.rooms[room_name].subscribers[uin] = nick

            logging.info(
                f"ChatRooms: загружено {len(self.user_room)} подписчиков, "
                f"{len(self.rooms)} комнат, {len(self.user_last_active)} временных меток"
            )
        except Exception as e:
            logging.error(f"ChatRooms: не удалось загрузить состояние: {e}")

    def _restore_active_users(self):
        now = time.time()
        for uin, last_active in self.user_last_active.items():
            room_name = self.user_room.get(uin)
            if room_name and room_name in self.rooms:
                room = self.rooms[room_name]
                nick = self.user_nick.get(uin, f"User{uin}")
                if (now - last_active) < IDLE_TIMEOUT:
                    room.active[uin] = nick

    def _save_state(self):
        try:
            rooms_data = [
                {
                    "name":          room.name,
                    "password_hash": room.password_hash,
                    "is_public":     room.is_public,
                }
                for room in self.rooms.values()
                if room.name not in self.PUBLIC_ROOMS
            ]
            data = {
                "user_nick":          self.user_nick,
                "user_room":          self.user_room,
                "user_last_active":   self.user_last_active,
                "user_last_reminded": self.user_last_reminded,
                "rooms":              rooms_data,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"ChatRooms: не удалось сохранить состояние: {e}")

    # ── Активность ────────────────────────────────────────────────────────────

    def touch(self, uin: str):
        now = time.time()
        self.user_last_active[uin]   = now
        self.user_last_reminded[uin] = now
        room = self.current_room(uin)
        if room:
            room.active[uin] = self.get_nick(uin)
        self._save_state()

    def check_idle_users(self) -> list[tuple[str, str]]:
        """Убирает из active тех, кто молчит дольше IDLE_TIMEOUT."""
        now = time.time()
        deactivated = []
        for uin, room_name in list(self.user_room.items()):
            last = self.user_last_active.get(uin, 0)
            room = self.rooms.get(room_name)
            if room and uin in room.active and (now - last) >= IDLE_TIMEOUT:
                room.active.pop(uin, None)
                deactivated.append((uin, room_name))
        if deactivated:
            self._save_state()
        return deactivated

    def check_remind_users(self) -> list[tuple[str, str]]:
        """Возвращает пользователей, которым пора напомнить о комнате."""
        now = time.time()
        to_remind = []
        for uin, room_name in list(self.user_room.items()):
            last_active   = self.user_last_active.get(uin, 0)
            last_reminded = self.user_last_reminded.get(uin, 0)
            if (now - last_active) >= REMIND_TIMEOUT and (now - last_reminded) >= REMIND_TIMEOUT:
                to_remind.append((uin, room_name))
                self.user_last_reminded[uin] = now
        if to_remind:
            self._save_state()
        return to_remind

    # ── Никнеймы ──────────────────────────────────────────────────────────────

    def get_nick(self, uin: str) -> str:
        return self.user_nick.get(uin, f"User{uin}")

    def set_nick(self, uin: str, nick: str) -> Optional[str]:
        nick = nick.strip()
        if not nick:
            return "Никнейм не может быть пустым."
        if len(nick) > 32:
            return "Никнейм слишком длинный (максимум 32 символа)."
        for u, n in self.user_nick.items():
            if u != uin and n.lower() == nick.lower():
                return f"Никнейм '{nick}' уже занят."
        old_nick = self.get_nick(uin)
        self.user_nick[uin] = nick
        room = self.current_room(uin)
        if room:
            if uin in room.subscribers:
                room.subscribers[uin] = nick
            if uin in room.active:
                room.active[uin] = nick
        self._save_state()
        return None

    # ── Операции с комнатами ──────────────────────────────────────────────────

    def current_room(self, uin: str) -> Optional[Room]:
        name = self.user_room.get(uin)
        return self.rooms.get(name) if name else None

    def is_in_room(self, uin: str, room_name: str) -> bool:
        return self.user_room.get(uin) == room_name

    def join_room(self, uin: str, room_name: str,
                  password: str = "") -> tuple[bool, str]:
        room_name = room_name.lower().strip()
        if room_name not in self.rooms:
            return False, f"Комната '{room_name}' не существует."
        if self.is_in_room(uin, room_name):
            return False, "Вы уже находитесь в этой комнате."
        room = self.rooms[room_name]
        if not room.check_password(password):
            return False, "Неверный пароль."
        self._leave_silent(uin)
        nick = self.get_nick(uin)
        room.subscribers[uin] = nick
        room.active[uin] = nick
        self.user_room[uin] = room_name
        now = time.time()
        self.user_last_active[uin]   = now
        self.user_last_reminded[uin] = now
        self._save_state()
        return True, room_name

    def leave_room(self, uin: str) -> Optional[str]:
        room_name = self.user_room.get(uin)
        if not room_name:
            return None
        self._leave_silent(uin)
        self._save_state()
        return room_name

    def _leave_silent(self, uin: str):
        room_name = self.user_room.pop(uin, None)
        if room_name and room_name in self.rooms:
            self.rooms[room_name].subscribers.pop(uin, None)
            self.rooms[room_name].active.pop(uin, None)
        self.user_last_active.pop(uin, None)
        self.user_last_reminded.pop(uin, None)

    def create_room(self, uin: str, room_name: str,
                    password: str = "") -> tuple[bool, str]:
        room_name = room_name.lower().strip()
        if not room_name:
            return False, "Имя комнаты не может быть пустым."
        if len(room_name) > 32:
            return False, "Имя комнаты слишком длинное (максимум 32 символа)."
        if not room_name.replace("_", "").replace("-", "").isalnum():
            return False, "Имя комнаты может содержать только буквы, цифры, - и _."
        if room_name in self.rooms:
            return False, f"Комната '{room_name}' уже существует."
        pw_hash = hashlib.sha256(password.encode()).hexdigest() if password else None
        self.rooms[room_name] = Room(
            name=room_name,
            password_hash=pw_hash,
            is_public=(not password),
        )
        self._save_state()
        return True, room_name

    def room_subscribers(self, room_name: str) -> list[str]:
        room = self.rooms.get(room_name)
        return list(room.subscribers.keys()) if room else []

    def room_active_members(self, room_name: str) -> list[str]:
        room = self.rooms.get(room_name)
        return list(room.active.values()) if room else []

    def list_rooms(self) -> str:
        lines = []
        for name, room in sorted(self.rooms.items()):
            lock   = "" if room.is_public else " 🔒"
            total  = len(room.subscribers)
            online = len(room.active)
            lines.append(f"  {name}{lock} ({online} онлайн, {total} в комнате)")
        return "\n".join(lines) if lines else "  (нет комнат)"


# ── Модуль ────────────────────────────────────────────────────────────────────

_manager         = ChatRoomManager()
_command_handler = None
_bot_ref         = None


def setup(handler):
    global _command_handler
    _command_handler = handler

    handler.register_command("nick",   nick_command)
    handler.register_command("rooms",  rooms_command)
    handler.register_command("join",   join_command)
    handler.register_command("create", create_command)
    handler.register_command("who",    who_command)
    handler.register_command("leave",  leave_command)
    handler.register_command("qwen",   qwen_room_command)

    handler.room_public_commands.add("weather")
    handler.room_public_commands.add("qwen")
    logging.info(f"ChatRooms: room_public_commands = {handler.room_public_commands}")

    handler.set_default_handler(chat_message_handler)

    asyncio.get_event_loop().create_task(_idle_checker())
    asyncio.get_event_loop().create_task(_remind_checker())


# ── Фоновые задачи ────────────────────────────────────────────────────────────

async def _idle_checker():
    """Каждую минуту убирает молчунов из active (без уведомлений в чат)."""
    while True:
        await asyncio.sleep(60)
        try:
            _manager.check_idle_users()
        except Exception as e:
            logging.error(f"Ошибка idle_checker: {e}")


async def _remind_checker():
    """Каждые 10 минут проверяет, кому пора напомнить о комнате (лично)."""
    while True:
        await asyncio.sleep(10 * 60)
        try:
            to_remind = _manager.check_remind_users()
            for uin, room_name in to_remind:
                if not _bot_ref:
                    continue
                nick         = _manager.get_nick(uin)
                online_count = len(_manager.room_active_members(room_name))
                msg = (
                    f"👋 Привет, {nick}! Напоминаю, что вы всё ещё в комнате «{room_name}».\n"
                    f"Сейчас онлайн: {online_count} чел.\n"
                    f"Напишите что-нибудь или /leave чтобы выйти."
                )
                await _bot_ref._send_message(uin, msg)
                logging.debug(f"ChatRooms: напоминание → {nick} ({uin}) о {room_name}")
        except Exception as e:
            logging.error(f"Ошибка remind_checker: {e}")


# ── Вспомогательные функции ───────────────────────────────────────────────────

async def _broadcast(bot, room_name: str, text: str, exclude_uin: str = None):
    global _bot_ref
    if bot:
        _bot_ref = bot
    if not bot:
        return
    targets = [u for u in _manager.room_subscribers(room_name) if u != exclude_uin]
    logging.debug(f"_broadcast -> {room_name}: {len(targets)} получателей")
    
    for uin in targets:
        await bot._send_message(uin, text)
        await asyncio.sleep(0.5)  # Задержка 500 мс между сообщениями


async def _broadcast_to_active(bot, room_name: str, text: str, exclude_uin: str = None):
    """Рассылает только активным участникам."""
    if not bot:
        return
    room = _manager.rooms.get(room_name)
    if not room:
        return
    targets = [u for u in list(room.active) if u != exclude_uin]
    tasks = [bot._send_message(u, text) for u in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_public_command_in_room(bot, uin: str, command: str, args: str):
    """
    Выполняет публичную команду в комнате:
    1. Анонсирует запрос всем подписчикам (кроме отправителя)
    2. Выполняет команду
    3. Рассылает ответ всем подписчикам
    Возвращает False если пользователь не в комнате.
    """
    room = _manager.current_room(uin)
    if not room:
        logging.debug(f"_run_public_command_in_room: {uin} не в комнате")
        return False

    nick = _manager.get_nick(uin)
    _manager.touch(uin)

    # Анонсируем запрос всем, КРОМЕ отправителя
    query_text = f"[{room.name}] {nick}: /{command}" + (f" {args}" if args else "")
    logging.info(f"Public command in {room.name}: {query_text}")
    await _broadcast(bot, room.name, query_text, exclude_uin=uin)

    handler_func = _command_handler.commands.get(command) if _command_handler else None
    if handler_func is None:
        answer = f"Команда /{command} не найдена."
        logging.warning(f"/{command} не найдена в commands")
    else:
        try:
            if asyncio.iscoroutinefunction(handler_func):
                answer = await handler_func(bot, uin, args)
            else:
                answer = handler_func(bot, uin, args)
        except Exception as e:
            logging.error(f"Ошибка публичной команды /{command}: {e}", exc_info=True)
            answer = f"Ошибка выполнения /{command}: {e}"

    if answer:
        response_msg = f"[{room.name}] Ответ для {nick}:\n{answer}"
        logging.info(f"Public command response in {room.name}: {response_msg[:100]}")
        await _broadcast(bot, room.name, response_msg)

    return None


# ── Обработчики команд ────────────────────────────────────────────────────────

async def qwen_room_command(bot, uin: str, args: str) -> str:
    """Обёртка над Qwen для использования в комнате как публичной команды."""
    if not args.strip():
        return "Использование: /qwen <вопрос>"
    if not _command_handler or not _command_handler.qwen:
        return "Qwen недоступен."
    return await _command_handler._call_qwen(uin, args)


async def nick_command(bot, uin: str, args: str) -> str:
    nick = args.strip()
    if not nick:
        return f"Ваш текущий никнейм: {_manager.get_nick(uin)}\nИспользование: /nick <имя>"
    old_nick = _manager.get_nick(uin)
    error = _manager.set_nick(uin, nick)
    if error:
        return error
    room = _manager.current_room(uin)
    if room:
        await _broadcast(bot, room.name,
                         f"* {old_nick} теперь известен как {nick}",
                         exclude_uin=uin)
    return f"Никнейм установлен: {nick}"


async def rooms_command(bot, uin: str, args: str) -> str:
    return f"Доступные комнаты:\n{_manager.list_rooms()}"


async def join_command(bot, uin: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    if not parts:
        return "Использование: /join <комната> [пароль]"
    room_name = parts[0]
    password  = parts[1] if len(parts) > 1 else ""
    old_room  = _manager.current_room(uin)
    ok, result = _manager.join_room(uin, room_name, password)
    if not ok:
        return result
    nick = _manager.get_nick(uin)
    if old_room:
        await _broadcast(bot, old_room.name, f"* {nick} покинул комнату.")
    await _broadcast(bot, result, f"* {nick} присоединился к комнате.", exclude_uin=uin)
    members     = _manager.room_active_members(result)
    members_str = ", ".join(members) if members else "(только вы)"
    total       = len(_manager.room_subscribers(result))
    lock_note   = "" if _manager.rooms[result].is_public else " (приватная)"
    return (f"Присоединились к комнате: {result}{lock_note}\n"
            f"Онлайн: {members_str}\n"
            f"Всего в комнате: {total}\n"
            f"Введите что угодно для чата, /leave для выхода.")


async def create_command(bot, uin: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    if not parts:
        return "Использование: /create <комната> [пароль]"
    room_name = parts[0]
    password  = parts[1] if len(parts) > 1 else ""
    ok, result = _manager.create_room(uin, room_name, password)
    if not ok:
        return result
    lock_note = f" с паролем '{password}'" if password else " (публичная)"
    old_room  = _manager.current_room(uin)
    ok2, result2 = _manager.join_room(uin, room_name, password)
    if not ok2:
        return f"Комната '{room_name}' создана{lock_note}, но не удалось присоединиться: {result2}"
    nick = _manager.get_nick(uin)
    if old_room:
        await _broadcast(bot, old_room.name, f"* {nick} покинул комнату.")
    await _broadcast(bot, room_name, f"* {nick} присоединился к комнате.", exclude_uin=uin)
    members     = _manager.room_active_members(room_name)
    members_str = ", ".join(members) if members else "(только вы)"
    total       = len(_manager.room_subscribers(room_name))
    return (f"Комната '{room_name}' создана{lock_note}.\n"
            f"Теперь вы в: {room_name}\n"
            f"Онлайн: {members_str}\n"
            f"Всего в комнате: {total}\n"
            f"Введите что угодно для чата, /leave для выхода.")


async def who_command(bot, uin: str, args: str) -> str:
    room = _manager.current_room(uin)
    if not room:
        return "Вы не в комнате. Используйте /join <комната> для входа."
    active     = _manager.room_active_members(room.name)
    total      = len(_manager.room_subscribers(room.name))
    active_str = "\n  ".join(active) if active else "(никого нет в сети прямо сейчас)"
    return (f"Комната: {room.name}\n"
            f"Онлайн ({len(active)}):\n  {active_str}\n"
            f"Всего в комнате: {total}")


async def leave_command(bot, uin: str, args: str) -> str:
    room_name = _manager.leave_room(uin)
    if not room_name:
        return "Вы не в комнате."
    nick = _manager.get_nick(uin)
    await _broadcast(bot, room_name, f"* {nick} покинул комнату.")
    return f"Покинули комнату: {room_name}"


# ── Обработчик по умолчанию ───────────────────────────────────────────────────

async def chat_message_handler(bot, uin: str, text: str) -> Optional[str]:
    if text.startswith("/"):
        parts   = text[1:].split(" ", 1)
        command = parts[0].lower()
        args    = parts[1] if len(parts) > 1 else ""
        return await _run_public_command_in_room(bot, uin, command, args)

    room = _manager.current_room(uin)
    if not room:
        return False

    _manager.touch(uin)
    nick = _manager.get_nick(uin)
    await _broadcast(bot, room.name, f"[{room.name}] {nick}: {text}", exclude_uin=uin)
    return None
