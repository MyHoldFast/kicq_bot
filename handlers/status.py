from command_handler import admin_only
def setup(handler):
    handler.register_command("status", status_command)
@admin_only()
async def status_command(bot, user_id: str, args: str) -> str:
    parts = args.split(maxsplit=1)
    status_name = parts[0] if parts else ""
    message = parts[1] if len(parts) > 1 else ""
    if not status_name:
        return "Usage: /status [online|away|dnd|free] [message]"
    try:
        await bot.set_status_by_name(status_name, message)
        return f"Status set to {status_name}"
    except ValueError as e:
        return str(e)