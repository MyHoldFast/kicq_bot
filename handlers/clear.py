_command_handler = None


def setup(handler):
    global _command_handler
    _command_handler = handler
    
    handler.register_command(
        "clear",
        clear_command,
        help_text="/clear - забыть историю разговора с DeepSeek",
        group="Основные"
    )
    handler.register_command("reset", clear_command)


async def clear_command(bot, user_id: str, args: str) -> str:
    deepseek = _command_handler.get_deepseek() if _command_handler else None
    if deepseek:
        deepseek.clear_context(user_id)
        return "Контекст очищен"
    return "DeepSeek not configured"