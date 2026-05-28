_command_handler = None


def setup(handler):
    global _command_handler
    _command_handler = handler
    
    handler.register_command(
        "help",
        help_command,
        help_text="/help - список всех команд",
        group="Основные"
    )
    handler.register_command("start", help_command)


def help_command(bot, user_id: str, args: str) -> str:
    return _command_handler.build_help_text()