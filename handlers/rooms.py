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

    def check_password(self, password: str) -> bool:
        if self.password_hash is None:
            return True
        return self.password_hash == hashlib.sha256(password.encode()).hexdigest()


class ChatRoomManager:
    PUBLIC_ROOMS = ["general", "offtopic", "tech"]

    def __init__(self):
        # Единственный источник истины: uin -> room_name
        self.user_room:        dict[str, str]   = {}
        self.user_nick:        dict[str, str]   = {}
        self.user_last_active: dict[str, float] = {}
        self.rooms:            dict[str, Room]  = {}

        for name in self.PUBLIC_ROOMS:
            self.rooms[name] = Room(name=name, is_public=True)

        self._load_state()

    # ── Персистентность ───────────────────────────────────────────────────────

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.user_nick        = {str(k): v for k, v in data.get("user_nick", {}).items()}
            self.user_last_active = {str(k): v for k, v in data.get("user_last_active", {}).items()}

            for rdata in data.get("rooms", []):
                name = rdata["name"]
                if name not in self.rooms:
                    self.rooms[name] = Room(
                        name=name,
                        password_hash=rdata.get("password_hash"),
                        is_public=rdata.get("is_public", True),
                    )

            for uin, room_name in data.get("user_room", {}).items():
                uin = str(uin)
                if room_name in self.rooms:
                    self.user_room[uin] = room_name

            logging.info(
                f"ChatRooms: загружено {len(self.user_room)} пользователей, "
                f"{len(self.rooms)} комнат"
            )
        except Exception as e:
            logging.error(f"ChatRooms: не удалось загрузить состояние: {e}")

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

    # ── Запросы (только читают user_room) ────────────────────────────────────

    def get_room_of(self, uin: str) -> Optional[Room]:
        """Комната, в которой сейчас находится пользователь."""
        room_name = self.user_room.get(uin)
        return self.rooms.get(room_name) if room_name else None

    def is_in_room(self, uin: str, room_name: str) -> bool:
        return self.user_room.get(uin) == room_name

    def members_of(self, room_name: str) -> list[str]:
        """UIN'ы всех кто сейчас в комнате."""
        return [u for u, r in self.user_room.items() if r == room_name]

    def active_members_of(self, room_name: str) -> list[str]:
        """Никнеймы тех, кто был активен в последние IDLE_TIMEOUT секунд."""
        now = time.time()
        result = []
        for uin, r in self.user_room.items():
            if r != room_name:
                continue
            last = self.user_last_active.get(uin, 0)
            if (now - last) < IDLE_TIMEOUT:
                result.append(self.get_nick(uin))
        return result

    def list_rooms(self) -> list[dict]:
        result = []
        for name, room in sorted(self.rooms.items()):
            all_members    = self.members_of(name)
            active_members = self.active_members_of(name)
            result.append({
                "name":     name,
                "password": room.password_hash is not None,
                "members":  len(all_members),
                "online":   len(active_members),
            })
        return result

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
        self.user_nick[uin] = nick
        self._save_state()
        return None

    # ── Операции ──────────────────────────────────────────────────────────────

    def join(self, uin: str, room_name: str, password: str = "") -> None:
        room_name = room_name.lower().strip()
        if room_name not in self.rooms:
            raise KeyError(f"Комната '{room_name}' не найдена")
        if self.is_in_room(uin, room_name):
            raise ValueError(f"already_in_room")
        room = self.rooms[room_name]
        if not room.check_password(password):
            raise PermissionError("Неверный пароль")
        self.user_room[uin] = room_name
        self.user_last_active[uin] = time.time()
        self._save_state()

    def leave(self, uin: str) -> str:
        room_name = self.user_room.pop(uin, None)
        if not room_name:
            raise KeyError("Пользователь не в комнате")
        self.user_last_active.pop(uin, None)
        self._save_state()
        return room_name

    def create(self, uin: str, room_name: str, password: str = "") -> None:
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

    def touch(self, uin: str) -> bool:
        """Обновляет время активности. Возвращает True если пора напомнить о комнате."""
        now  = time.time()
        last = self.user_last_active.get(uin, 0)
        should_remind = (last > 0) and (now - last >= REMIND_TIMEOUT)
        self.user_last_active[uin] = now
        self._save_state()
        return should_remind


# ── Глобальные переменные ─────────────────────────────────────────────────────

_manager:         ChatRoomManager = ChatRoomManager()
_command_handler                  = None


# ── Вспомогательные функции ───────────────────────────────────────────────────

async def _broadcast(bot, room_name: str, text: str, exclude_uin: str = None):
    """Рассылка сообщения всем участникам комнаты."""
    if not bot:
        return
    targets = [u for u in _manager.members_of(room_name) if u != exclude_uin]
    logging.debug(f"_broadcast -> {room_name}: {len(targets)} получателей")
    for uin in targets:
        try:
            await bot.send_message(uin, text)
        except Exception as e:
            logging.warning(f"_broadcast: failed for {uin}: {e}")
        await asyncio.sleep(0.5)


async def _run_public_command_in_room(bot, uin: str, command: str, args: str):
    room = _manager.get_room_of(uin)
    if not room:
        logging.debug(f"_run_public_command_in_room: {uin} не в комнате")
        return False

    nick = _manager.get_nick(uin)
    _manager.touch(uin)

    query_text = f"[{room.name}] {nick}: /{command}" + (f" {args}" if args else "")
    logging.info(f"Public command in {room.name}: {query_text}")

    def get_targets():
        return [u for u in _manager.members_of(room.name) if u != uin]

    async def broadcast_typing(is_typing: bool):
        for target in get_targets():
            try:
                await bot.send_typing(target, is_typing)
            except Exception:
                pass

    async def keep_typing():
        try:
            while True:
                await asyncio.sleep(5)
                await broadcast_typing(True)
        except asyncio.CancelledError:
            pass

    await broadcast_typing(True)
    await _broadcast(bot, room.name, query_text, exclude_uin=uin)
    await broadcast_typing(True)

    typing_task = asyncio.create_task(keep_typing())

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

    typing_task.cancel()
    await asyncio.gather(typing_task, return_exceptions=True)
    await broadcast_typing(False)

    if answer:
        response_msg = f"[{room.name}] Ответ для {nick}:\n{answer}"
        logging.info(f"Public command response in {room.name}: {response_msg[:100]}")
        await _broadcast(bot, room.name, response_msg)

    return None


# ── Обработчики команд ────────────────────────────────────────────────────────

async def qwen_room_command(bot, uin: str, args: str) -> str:
    if not args.strip():
        return "Использование: /qwen <вопрос>"
    if not _command_handler or not _command_handler.get_qwen():
        return "Qwen недоступен."
    return await _command_handler.call_qwen(uin, args)


async def nick_command(bot, uin: str, args: str) -> str:
    nick = args.strip()
    if not nick:
        return f"Ваш текущий никнейм: {_manager.get_nick(uin)}\nИспользование: /nick <имя>"
    old_nick = _manager.get_nick(uin)
    error = _manager.set_nick(uin, nick)
    if error:
        return error
    room = _manager.get_room_of(uin)
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
    room_name = parts[0].lower().strip()
    password  = parts[1] if len(parts) > 1 else ""

    old_room = _manager.get_room_of(uin)

    try:
        _manager.join(uin, room_name, password)
    except ValueError:
        # already_in_room
        members     = _manager.active_members_of(room_name)
        members_str = ", ".join(members) if members else "(только вы)"
        total       = len(_manager.members_of(room_name))
        return (f"Вы уже находитесь в комнате '{room_name}'.\n"
                f"Онлайн: {members_str}\n"
                f"Всего в комнате: {total}")
    except PermissionError:
        return "Неверный пароль."
    except KeyError:
        return f"Комната '{room_name}' не найдена. /rooms — список комнат."

    nick = _manager.get_nick(uin)
    if old_room:
        await _broadcast(bot, old_room.name, f"* {nick} покинул комнату.")
    await _broadcast(bot, room_name, f"* {nick} присоединился к комнате.", exclude_uin=uin)

    members     = _manager.active_members_of(room_name)
    members_str = ", ".join(members) if members else "(только вы)"
    total       = len(_manager.members_of(room_name))
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
    old_room  = _manager.get_room_of(uin)
    try:
        _manager.join(uin, room_name, password)
    except Exception as e:
        return f"Комната '{room_name}' создана{lock_note}, но не удалось присоединиться: {e}"
    nick = _manager.get_nick(uin)
    if old_room:
        await _broadcast(bot, old_room.name, f"* {nick} покинул комнату.")
    await _broadcast(bot, room_name, f"* {nick} присоединился к комнате.", exclude_uin=uin)
    members     = _manager.active_members_of(room_name)
    members_str = ", ".join(members) if members else "(только вы)"
    total       = len(_manager.members_of(room_name))
    return (f"Комната '{room_name}' создана{lock_note}.\n"
            f"Теперь вы в: {room_name}\n"
            f"Онлайн: {members_str}\n"
            f"Всего в комнате: {total}\n"
            f"Введите что угодно для чата, /leave для выхода.")


async def who_command(bot, uin: str, args: str) -> str:
    room = _manager.get_room_of(uin)
    if not room:
        return "Вы не в комнате. Используйте /join <комната> для входа."
    active     = _manager.active_members_of(room.name)
    total      = len(_manager.members_of(room.name))
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

    room = _manager.get_room_of(uin)
    if not room:
        return False

    should_remind = _manager.touch(uin)
    nick = _manager.get_nick(uin)

    # Сбрасываем typing у всех остальных перед отправкой сообщения
    for target in [u for u in _manager.members_of(room.name) if u != uin]:
        try:
            await bot.send_typing(target, False)
        except Exception:
            pass

    await _broadcast(bot, room.name, f"[{room.name}] {nick}: {text}", exclude_uin=uin)

    if should_remind:
        online_count = len(_manager.active_members_of(room.name))
        await bot.send_message(uin, (
            f"Напоминание: вы находитесь в комнате '{room.name}'.\n"
            f"Сейчас онлайн: {online_count} чел.\n"
            f"Напишите /leave чтобы выйти."
        ))

    return None


# ── Обработчик typing ─────────────────────────────────────────────────────────

async def handle_typing(bot, uin: str, is_typing: bool):
    """Проксирует typing-уведомление от участника комнаты остальным её членам."""
    room = _manager.get_room_of(uin)
    if not room:
        return
    targets = [u for u in _manager.members_of(room.name) if u != uin]
    for target in targets:
        try:
            await bot.send_typing(target, is_typing)
        except Exception as e:
            logging.warning(f"handle_typing: failed for {target}: {e}")


# ── Фоновые задачи ────────────────────────────────────────────────────────────

async def _idle_checker():
    """Каждую минуту проверяет молчунов (для active_members_of, данные не чистим — просто время)."""
    while True:
        await asyncio.sleep(60)


# ── Инициализация модуля ──────────────────────────────────────────────────────

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

    asyncio.get_event_loop().create_task(_idle_checker())