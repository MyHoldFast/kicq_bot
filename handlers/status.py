from command_handler import admin_only
from icq_core import Status


def setup(handler):
    handler.register_command(
        "status",
        status_command,
        # help_text="/status [online|away|dnd|free] [текст] - статус бота",
        # group="Администратор"
    )


@admin_only()
async def status_command(bot, user_id: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    status_name = parts[0] if parts else ""
    message = parts[1] if len(parts) > 1 else ""
    
    if not status_name:
        return "Usage: /status [online|away|dnd|free] [message]"
    
    status_map = {
        "online": Status.ONLINE,
        "away": Status.AWAY,
        "dnd": Status.DND,
        "free": Status.FREE,
    }
    
    if status_name.lower() not in status_map:
        return f"Неизвестный статус. Доступные: online, away, dnd, free"
    
    await bot.set_status(status_map[status_name.lower()], message)
    return f"Status set to {status_name}"