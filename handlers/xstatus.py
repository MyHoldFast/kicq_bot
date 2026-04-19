from command_handler import admin_only

def setup(handler):
    handler.register_command("xstatus", xstatus_command)

#@admin_only("3142384")
async def xstatus_command(bot, user_id: str, args: str) -> str:
    if not args:
        return "Usage: /xstatus [name|none] - Use /xlist to see available XStatuses"
    try:
        await bot.set_xstatus(args)
        if args.lower() == "none":
            return "XStatus cleared"
        else:
            return f"XStatus set to {args} (broadcasted to contacts)"
    except ValueError as e:
        return str(e)