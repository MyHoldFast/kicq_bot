import os
import json
import importlib.util
import asyncio
import logging
from typing import Dict, Callable, Optional, Set

_DB_DIR       = "db"
_SEEN_DB_PATH = os.path.join(_DB_DIR, "seen_users.json")

WELCOME_AND_HELP = """\
Привет! Я бот на базе Qwen AI.
Просто пиши мне - отвечу как обычному собеседнику.
Или используй команды:

/help - эта справка
/clear - забыть историю разговора с Qwen
/weather Москва - погода (город запомнится)

Чат-комнаты:
/nick <имя> - установить имя в чате
/rooms - какие есть комнаты
/join <комната> [пароль] - зайти в комнату
/create <комната> [пароль] - создать свою комнату
/who - кто сейчас в комнате
/leave - выйти из комнаты

В личке Qwen отвечает сам.
В комнате: /qwen <вопрос>
/weather <город>
"""


def _load_seen_users() -> set:
    try:
        if os.path.exists(_SEEN_DB_PATH):
            with open(_SEEN_DB_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
    except Exception as e:
        logging.error(f"Failed to load seen_users: {e}")
    return set()


def _save_seen_users(seen: set):
    try:
        os.makedirs(_DB_DIR, exist_ok=True)
        with open(_SEEN_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(seen), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Failed to save seen_users: {e}")


class CommandHandler:
    def __init__(self):
        self.commands: Dict[str, Callable] = {}
        self.default_handler: Optional[Callable] = None
        self.qwen = None
        self.active_requests: Dict[str, asyncio.Task] = {}
        self.room_public_commands: Set[str] = set()

        self._seen_users: set = _load_seen_users()
        logging.info(f"Seen users loaded: {len(self._seen_users)} entries")

    def register_command(self, command: str, handler: Callable):
        self.commands[command] = handler

    def set_default_handler(self, handler: Callable):
        self.default_handler = handler

    def register_qwen(self, api_key: str):
        from qwen_handler import QwenHandler
        self.qwen = QwenHandler(api_key=api_key)

    def load_commands_from_directory(self, directory: str):
        if not os.path.exists(directory):
            logging.warning(f"Commands directory {directory} not found")
            return
        for filename in sorted(os.listdir(directory)):
            if filename.endswith('.py') and filename != '__init__.py':
                module_name = filename[:-3]
                module_path = os.path.join(directory, filename)
                try:
                    spec   = importlib.util.spec_from_file_location(module_name, module_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    if hasattr(module, 'setup'):
                        module.setup(self)
                        logging.info(f"Loaded command module: {module_name}")
                    else:
                        logging.warning(f"Module {module_name} has no setup() function")
                except Exception as e:
                    logging.error(f"Failed to load command module {filename}: {e}")

    def _is_new_user(self, user_id: str) -> bool:
        if user_id in self._seen_users:
            return False
        self._seen_users.add(user_id)
        _save_seen_users(self._seen_users)
        logging.info(f"New user: {user_id}")
        return True

    async def _call_qwen(self, user_id: str, message: str) -> Optional[str]:
        if not self.qwen:
            return None
        if user_id in self.active_requests:
            task = self.active_requests[user_id]
            if not task.done():
                return "Подождите, предыдущий запрос ещё обрабатывается..."
        task = asyncio.create_task(self.qwen.process_message(user_id, message))
        self.active_requests[user_id] = task
        try:
            return await task
        except asyncio.CancelledError:
            return "Запрос отменён."
        except Exception as e:
            logging.error(f"Qwen processing error: {e}")
            return f"Ошибка: {e}"
        finally:
            self.active_requests.pop(user_id, None)

    async def handle_message_async(self, bot, user_id: str, message: str) -> Optional[str]:
        if self._is_new_user(user_id):
            return WELCOME_AND_HELP

        if message.startswith('/'):
            parts   = message[1:].split(' ', 1)
            command = parts[0].lower()
            args    = parts[1] if len(parts) > 1 else ""

            if self.default_handler and command in self.room_public_commands:
                logging.debug(f"Public room command intercepted: /{command} from {user_id}")
                try:
                    if asyncio.iscoroutinefunction(self.default_handler):
                        result = await self.default_handler(bot, user_id, message)
                    else:
                        result = self.default_handler(bot, user_id, message)
                    if result is not False:
                        return result
                except Exception as e:
                    logging.error(f"Default handler error (public command): {e}")
                    return f"Error: {str(e)}"

            if command == "qwen":
                return await self._call_qwen(user_id, args)

            if command in self.commands:
                handler = self.commands[command]
                try:
                    if asyncio.iscoroutinefunction(handler):
                        return await handler(bot, user_id, args)
                    else:
                        return handler(bot, user_id, args)
                except Exception as e:
                    logging.error(f"Command handler error: {e}")
                    return f"Error: {str(e)}"
            return None

        if self.default_handler:
            try:
                if asyncio.iscoroutinefunction(self.default_handler):
                    result = await self.default_handler(bot, user_id, message)
                else:
                    result = self.default_handler(bot, user_id, message)
                if result is not False:
                    return result
            except Exception as e:
                logging.error(f"Default handler error: {e}")
                return f"Error: {str(e)}"

        return await self._call_qwen(user_id, message)


def admin_only(func=None):
    def decorator(f):
        async def wrapper(bot, user_id: str, args: str) -> str:
            admin_uin = os.environ.get("ADMIN_UIN", "")
            if user_id != admin_uin:
                return ""
            return await f(bot, user_id, args)
        wrapper.__wrapped__ = f
        return wrapper
    
    if func is not None:
        return decorator(func)
    return decorator
