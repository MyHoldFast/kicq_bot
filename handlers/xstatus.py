from command_handler import admin_only
from icq_core import XSTATUS_BY_NAME


def setup(handler):
    handler.register_command(
        "xstatus",
        xstatus_command,
        # help_text="/xstatus <имя|none> - установить/сбросить xstatus бота",
        # group="Администратор"
    )


@admin_only()
async def xstatus_command(bot, user_id: str, args: str) -> str:
    if not args:
        return "Usage: /xstatus [name|none] - Use /xlist to see available XStatuses"
    
    if args.lower() == "none":
        await bot.set_xstatus("", "", "")
        return "XStatus cleared"
    
    # Проверяем, существует ли такой xstatus
    if args.lower() not in XSTATUS_BY_NAME:
        return f"Unknown xstatus: {args}. Use /xlist to see available."
    
    # Устанавливаем xstatus с title и desc (используем args как title)
    await bot.set_xstatus(args.lower(), args, "")
    return f"XStatus set to {args} (broadcasted to contacts)"