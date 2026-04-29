#!/usr/bin/env python3
import asyncio
import logging
import os
from dotenv import load_dotenv
from icq_protocol import AsyncICQEchoBot, SERVER, PORT, XSTATUS_GUIDS
from command_handler import CommandHandler

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

async def main():
    UIN          = os.environ["UIN"]
    PASSWORD     = os.environ["PASSWORD"]
    QWEN_API_KEY = os.environ["QWEN_API_KEY"]

    bot = AsyncICQEchoBot(SERVER, PORT, UIN, PASSWORD)

    bot.current_xstatus = "beer"
    bot.set_xtraz_text("На меня глядит игриво...", "Пиво пиво пиво пиво")
    bot.xstatus_guid = bytes.fromhex(XSTATUS_GUIDS[bot.current_xstatus])
    

    command_handler = CommandHandler()
    bot.command_handler = command_handler

    command_handler.register_qwen(QWEN_API_KEY)
    logging.info("Qwen AI handler registered")

    command_handler.load_commands_from_directory("handlers")

    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
    