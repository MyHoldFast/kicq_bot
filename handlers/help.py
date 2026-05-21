def setup(handler):
    handler.register_command(
        "help",
        help_command,
        help_text="/help - список всех команд",
        group="Основные"
    )
    handler.register_command("start", help_command)  # без help_text — не дублируем в справке


def help_command(bot, user_id: str, args: str) -> str:
    return bot.command_handler.build_help_text()