_command_handler = None


def setup(handler):
    global _command_handler
    _command_handler = handler
    
    handler.register_command(
        "clear",
        clear_command,
        help_text="/clear - забыть историю разговора с Qwen",
        group="Основные"
    )
    handler.register_command("reset", clear_command)


async def clear_command(bot, user_id: str, args: str) -> str:
    qwen = _command_handler.get_qwen() if _command_handler else None
    if qwen:
        qwen.clear_context(user_id)
        return "Контекст очищен"
    return "Qwen not configured"