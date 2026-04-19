def setup(handler):
    handler.register_command("clear", clear_command)
    handler.register_command("reset", clear_command)

async def clear_command(bot, user_id: str, args: str) -> str:
    if bot.command_handler.qwen:
        bot.command_handler.qwen.clear_context(user_id)
        return "Контекст очищен"
    return "Qwen not configured"