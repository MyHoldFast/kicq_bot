def setup(handler):
    handler.register_command(
        "clear",
        clear_command,
        help_text="/clear - забыть историю разговора с Qwen",
        group="Основные"
    )
    handler.register_command("reset", clear_command)  # алиас без дублирования в справке


async def clear_command(bot, user_id: str, args: str) -> str:
    if bot.command_handler.qwen:
        bot.command_handler.qwen.clear_context(user_id)
        return "Контекст очищен"
    return "Qwen not configured"