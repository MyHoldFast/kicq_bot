from icq_protocol import XSTATUS_GUIDS

def setup(handler):
    handler.register_command("help", help_command)
    handler.register_command("start", help_command)

def help_command(bot, user_id: str, args: str) -> str:
    return f"""Основные команды:

/help — эта справка
/clear — забыть историю разговора с Qwen
/weather Москва — погода (город запомнится)

Чат-комнаты:
/nick <имя> — установить имя в чате (пример: /nick Иван)
/rooms — какие есть комнаты
/join <комната> [пароль] — зайти в комнату (пример: /join general)
/create <комната> [пароль] — создать свою комнату (пример: /create своя)
/who — кто сейчас в комнате
/leave — выйти из комнаты

Qwen отвечает сам, когда пишете в личку.
В общей комнате зовите его так: /qwen <вопрос> (пример: /qwen сколько будет 2+2)
Погода в комнате: /weather <город> (увидят все, пример: /weather Питер)"""