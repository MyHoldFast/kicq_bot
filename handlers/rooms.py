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

            self.user_nick        = data.get("user_nick", {})
            self.user_last_active = data.get("user_last_active", {})

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
                f"{len(self.rooms)} комнат"
            )
        except Exception as e:
            logging.error(f"ChatRooms: не удалось загрузить состояние: {e}")

    def _restore_active_users(self):
        now = time.time()
        for uin, last_active in self.user_last_active.items():
            room_name = self.user_room.get(uin)
            if room_name and room_name in self.rooms:
                if (now - last_active) < IDLE_TIMEOUT:
                    nick = self.user_nick.get(uin, f"User{uin}")
                    self.rooms[room_name].active[uin] = nick

    def _save_state(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
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
                "user_nick":        self.user_nick,
                "user_room":        self.user_room,
                "user_last_active": self.user_last_active,
                "rooms":            rooms_data,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"ChatRooms: не удалось сохранить состояние: {e}")

    # ── Активность ────────────────────────────────────────────────────────────

    def touch(self, uin: str) -> bool:
        """
        Обновляет время активности пользователя.
        Возвращает True если до этого молчал дольше REMIND_TIMEOUT —
        значит нужно прислать ему напоминание о комнате.
        """
        now  = time.time()
        last = self.user_last_active.get(uin, 0)
        should_remind = (last > 0) and (now - last >= REMIND_TIMEOUT)

        self.user_last_active[uin] = now
        room = self.current_room(uin)
        if room:
            room.active[uin] = self.get_nick(uin)
        self._save_state()
        return should_remind

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

    def join(self, uin: str, room_name: str, password: str = "") -> None:
        """Присоединиться к комнате. Поднимает KeyError или PermissionError при ошибке."""
        room_name = room_name.lower().strip()
        if room_name not in self.rooms:
            raise KeyError(f"Комната '{room_name}' не найдена")
        if self.is_in_room(uin, room_name):
            return
        room = self.rooms[room_name]
        if not room.check_password(password):
            raise PermissionError("Неверный пароль")
        self._leave_silent(uin)
        nick = self.get_nick(uin)
        room.subscribers[uin] = nick
        room.active[uin] = nick
        self.user_room[uin] = room_name
        self._save_state()

    def create(self, uin: str, room_name: str, password: str = "") -> None:
        """Создать комнату. Поднимает ValueError при ошибке валидации."""
        room_name = room_name.lower().strip()
        if not room_name:
            raise ValueError("Имя комнаты не может быть пустым.")
        if len(room_name) > 32:
            raise ValueError("Имя комнаты слишком длинное (максимум 32 символа).")
        if not room_name.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Имя комнаты может содержать только буквы, цифры, - и _.")
        if room_name in self.rooms:
            raise ValueError(f"Комната '{room_name}' уже существует.")
        pw_hash = hashlib.sha256(password.encode()).hexdigest() if password else None
        self.rooms[room_name] = Room(
            name=room_name,
            password_hash=pw_hash,
            is_public=(not password),
        )
        self._save_state()

    def leave(self, uin: str) -> str:
        """Выйти из комнаты. Поднимает KeyError если не в комнате. Возвращает имя комнаты."""
        room_name = self.user_room.get(uin)
        if not room_name:
            raise KeyError("Пользователь не в комнате")
        self._leave_silent(uin)
        self._save_state()
        return room_name

    def who(self, uin: str) -> Optional[list[str]]:
        """Список участников комнаты пользователя, или None если не в комнате."""
        room = self.current_room(uin)
        if not room:
            return None
        return list(room.subscribers.values())

    def list_rooms(self) -> list[dict]:
        """Список комнат с информацией."""
        result = []
        for name, room in sorted(self.rooms.items()):
            result.append({
                "name":     name,
                "password": room.password_hash is not None,
                "members":  len(room.subscribers),
                "online":   len(room.active),
            })
        return result

    def _leave_silent(self, uin: str):
        room_name = self.user_room.pop(uin, None)
        if room_name and room_name in self.rooms:
            self.rooms[room_name].subscribers.pop(uin, None)
            self.rooms[room_name].active.pop(uin, None)
        self.user_last_active.pop(uin, None)

    def room_subscribers(self, room_name: str) -> list[str]:
        room = self.rooms.get(room_name)
        return list(room.subscribers.keys()) if room else []

    def room_active_members(self, room_name: str) -> list[str]:
        room = self.rooms.get(room_name)
        return list(room.active.values()) if room else []


# ── Модуль ────────────────────────────────────────────────────────────────────

_manager         = ChatRoomManager()
_command_handler = None
_bot_ref         = None


def setup(handler):
    global _command_handler
    _command_handler = handler

    handler.register_command("nick",   nick_command,
                             help_text="/nick <имя> - установить имя в чате",
                             group="Чат-комнаты")
    handler.register_command("rooms",  rooms_command,
                             help_text="/rooms - список доступных комнат",
                             group="Чат-комнаты")
    handler.register_command("join",   join_command,
                             help_text="/join <комната> [пароль] - зайти в комнату",
                             group="Чат-комнаты")
    handler.register_command("create", create_command,
                             help_text="/create <комната> [пароль] - создать комнату",
                             group="Чат-комнаты")
    handler.register_command("who",    who_command,
                             help_text="/who - кто сейчас в комнате",
                             group="Чат-комнаты")
    handler.register_command("leave",  leave_command,
                             help_text="/leave - выйти из комнаты",
                             group="Чат-комнаты")
    handler.register_command("qwen",   qwen_room_command,
                             help_text="/qwen <вопрос> - спросить у Qwen (видно всем в комнате)",
                             group="Чат-комнаты")

    handler.room_public_commands.add("weather")
    handler.room_public_commands.add("qwen")
    logging.info(f"ChatRooms: room_public_commands = {handler.room_public_commands}")

    handler.set_default_handler(chat_message_handler)
    handler.bot.typing_handler = _on_typing

    asyncio.get_event_loop().create_task(_idle_checker())


# ── Фоновые задачи ────────────────────────────────────────────────────────────

async def _idle_checker():
    """Каждую минуту убирает молчунов из active (без уведомлений в чат)."""
    while True:
        await asyncio.sleep(60)
        try:
            _manager.check_idle_users()
        except Exception as e:
            logging.error(f"Ошибка idle_checker: {e}")


# ── Вспомогательные функции ───────────────────────────────────────────────────

async def _broadcast(bot, room_name: str, text: str, exclude_uin: str = None):
    """Рассылка сообщения всем подписчикам комнаты (включая неактивных)."""
    global _bot_ref
    if bot:
        _bot_ref = bot
    if not bot:
        return
    targets = [u for u in _manager.room_subscribers(room_name) if u != exclude_uin]
    logging.debug(f"_broadcast -> {room_name}: {len(targets)} получателей")
    for uin in targets:
        await bot._send_message(uin, text)
        await asyncio.sleep(0.5)


async def _broadcast_to_active(bot, room_name: str, text: str, exclude_uin: str = None):
    """Рассылает только активным участникам (кто писал в последние 5 минут)."""
    if not bot:
        return
    room = _manager.rooms.get(room_name)
    if not room:
        return
    targets = [u for u in list(room.active) if u != exclude_uin]
    tasks = [bot._send_message(u, text) for u in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def _on_typing(bot, uin: str, is_typing: bool):
    """Проксирует typing-уведомление от участника комнаты остальным её членам."""
    room = _manager.current_room(uin)
    if not room:
        return
    targets = [u for u in _manager.room_subscribers(room.name) if u != uin]
    for target in targets:
        try:
            await bot.send_typing(target, is_typing)
        except Exception as e:
            logging.warning(f"_on_typing proxy failed for {target}: {e}")


async def _run_public_command_in_room(bot, uin: str, command: str, args: str):
    room = _manager.current_room(uin)
    if not room:
        logging.debug(f"_run_public_command_in_room: {uin} не в комнате")
        return False

    nick = _manager.get_nick(uin)
    _manager.touch(uin)

    query_text = f"[{room.name}] {nick}: /{command}" + (f" {args}" if args else "")
    logging.info(f"Public command in {room.name}: {query_text}")

    targets = list(_manager.room_subscribers(room.name))

    async def broadcast_typing(is_typing: bool):
        for target in targets:
            try:
                await bot.send_typing(target, is_typing)
            except Exception:
                pass

    # Рассылаем анонс и включаем typing параллельно
    await asyncio.gather(
        _broadcast(bot, room.name, query_text, exclude_uin=uin),
        broadcast_typing(True),
    )

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

    # Гасим typing и рассылаем ответ параллельно
    if answer:
        response_msg = f"[{room.name}] Ответ для {nick}:\n{answer}"
        logging.info(f"Public command response in {room.name}: {response_msg[:100]}")
        await asyncio.gather(
            broadcast_typing(False),
            _broadcast(bot, room.name, response_msg),
        )
    else:
        await broadcast_typing(False)

    return None


# ── Обработчики команд ────────────────────────────────────────────────────────

async def qwen_room_command(bot, uin: str, args: str) -> str:
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
    room_list = _manager.list_rooms()
    if not room_list:
        return "Нет доступных комнат. Создайте свою: /create <название>"
    lines = ["Доступные комнаты:"]
    for r in room_list:
        lock = " [пароль]" if r["password"] else ""
        lines.append(f"  {r['name']}{lock} ({r['online']} онлайн, {r['members']} в комнате)")
    return "\n".join(lines)


async def join_command(bot, uin: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    if not parts:
        return "Использование: /join <комната> [пароль]"
    room_name = parts[0]
    password  = parts[1] if len(parts) > 1 else ""
    old_room  = _manager.current_room(uin)
    try:
        _manager.join(uin, room_name, password)
    except PermissionError:
        return "Неверный пароль."
    except KeyError:
        return f"Комната '{room_name}' не найдена. /rooms — список комнат."
    nick = _manager.get_nick(uin)
    if old_room:
        await _broadcast(bot, old_room.name, f"* {nick} покинул комнату.")
    await _broadcast(bot, room_name, f"* {nick} присоединился к комнате.", exclude_uin=uin)
    members     = _manager.room_active_members(room_name)
    members_str = ", ".join(members) if members else "(только вы)"
    total       = len(_manager.room_subscribers(room_name))
    lock_note   = " (приватная, с паролем)" if not _manager.rooms[room_name].is_public else ""
    return (f"Присоединились к комнате: {room_name}{lock_note}\n"
            f"Онлайн: {members_str}\n"
            f"Всего в комнате: {total}\n"
            f"Введите что угодно для чата, /leave для выхода.")


async def create_command(bot, uin: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    if not parts:
        return "Использование: /create <комната> [пароль]"
    room_name = parts[0]
    password  = parts[1] if len(parts) > 1 else ""
    try:
        _manager.create(uin, room_name, password)
    except ValueError as e:
        return str(e)
    lock_note = " с паролем" if password else " (публичная)"
    old_room  = _manager.current_room(uin)
    try:
        _manager.join(uin, room_name, password)
    except Exception as e:
        return f"Комната '{room_name}' создана{lock_note}, но не удалось присоединиться: {e}"
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
    active_str = "\n  * ".join(active) if active else "(никого нет в сети прямо сейчас)"
    return (f"Комната: {room.name}\n"
            f"Онлайн ({len(active)}):\n  * {active_str}\n"
            f"Всего в комнате: {total}")


async def leave_command(bot, uin: str, args: str) -> str:
    try:
        room_name = _manager.leave(uin)
    except KeyError:
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

    should_remind = _manager.touch(uin)
    nick = _manager.get_nick(uin)

    # Сбрасываем typing у всех перед отправкой сообщения
    targets = [u for u in _manager.room_subscribers(room.name) if u != uin]
    for target in targets:
        try:
            await bot.send_typing(target, False)
        except Exception:
            pass

    await _broadcast(bot, room.name, f"[{room.name}] {nick}: {text}", exclude_uin=uin)

    if should_remind and bot:
        online_count = len(_manager.room_active_members(room.name))
        await bot._send_message(uin, (
            f"Напоминание: вы находитесь в комнате '{room.name}'.\n"
            f"Сейчас онлайн: {online_count} чел.\n"
            f"Напишите /leave чтобы выйти."
        ))

    return None