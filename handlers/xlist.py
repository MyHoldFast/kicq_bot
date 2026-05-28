from command_handler import admin_only
from icq_core import XSTATUS_TABLE


def setup(handler):
    handler.register_command(
        "xlist",
        xlist_command,
        # help_text="/xlist - список доступных xstatus-ов",
        # group="Администратор"
    )


@admin_only()
async def xlist_command(bot, user_id: str, args: str) -> str:
    xstatuses = [name for guid, name in XSTATUS_TABLE if name != "unknown"]
    chunk_size = 15
    result = "Available XStatuses:\n"
    for i in range(0, len(xstatuses), chunk_size):
        result += ", ".join(xstatuses[i:i + chunk_size]) + "\n"
    result += "\nUse /xstatus [name] to set, or /xstatus none to clear"
    return result