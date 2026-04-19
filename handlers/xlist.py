from command_handler import admin_only
from icq_protocol import XSTATUS_GUIDS


def setup(handler):
    handler.register_command("xlist", xlist_command)
@admin_only()
def xlist_command(bot, user_id: str, args: str) -> str:
    xstatuses = list(XSTATUS_GUIDS.keys())
    xstatuses.remove('none')
    chunk_size = 15
    result = "Available XStatuses:\n"
    for i in range(0, len(xstatuses), chunk_size):
        result += ", ".join(xstatuses[i:i+chunk_size]) + "\n"
    result += "\nUse /xstatus [name] to set, or /xstatus none to clear"
    return result