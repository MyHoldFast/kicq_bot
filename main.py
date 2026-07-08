#!/usr/bin/env python3
import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from icq_core import ICQClient, Status, Message, AuthError
from command_handler import CommandHandler

load_dotenv()

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


async def main():
    UIN          = os.environ["UIN"]
    PASSWORD     = os.environ["PASSWORD"]
    QWEN_API_KEY = os.environ["QWEN_API_KEY"]

    command_handler = CommandHandler()
    command_handler.register_qwen(QWEN_API_KEY)
    logging.info("Qwen AI handler registered")
    command_handler.load_commands_from_directory("handlers")

    handle_typing = sys.modules["rooms"].handle_typing

    RECONNECT_DELAY_MIN = 5
    RECONNECT_DELAY_MAX = 300
    delay = RECONNECT_DELAY_MIN

    while True:
        client = ICQClient(UIN, PASSWORD)
        command_handler.bot = client
        last_error = None

        await client.set_xstatus("duck", "Уточка", "")
        await client.set_status(Status.FREE)

        semaphore = asyncio.Semaphore(3)
        active_tasks: set[asyncio.Task] = set()

        async def on_message(msg: Message):
            if msg.is_outgoing:
                return

            async def process():
                try:
                    await asyncio.wait_for(semaphore.acquire(), timeout=0.1)
                except asyncio.TimeoutError:
                    await client.send_message(
                        msg.sender_uin,
                        "Система перегружена, попробуйте позже."
                    )
                    return
                try:
                    await client.send_typing(msg.sender_uin, True)
                    response = await command_handler.handle_message_async(
                        client, msg.sender_uin, msg.text
                    )
                    if response:
                        await client.send_message(msg.sender_uin, response)
                except Exception as e:
                    logging.error(f"Error processing message from {msg.sender_uin}: {e}", exc_info=True)
                finally:
                    semaphore.release()
                    try:
                        await client.send_typing(msg.sender_uin, False)
                    except Exception:
                        pass

            task = asyncio.create_task(process())
            active_tasks.add(task)
            task.add_done_callback(active_tasks.discard)

        def on_connected():
            nonlocal delay
            delay = RECONNECT_DELAY_MIN
            logging.info("Connected to ICQ server")

        def on_disconnected():
            logging.info("Disconnected from ICQ server")

        def on_roster(groups, contacts):
            logging.info(f"Roster loaded: {len(groups)} groups, {len(contacts)} contacts")

        def on_contact_online(contact):
            logging.debug(f"{contact.display_name} ({contact.uin}) online: {contact.status.label}")

        def on_contact_offline(contact):
            logging.debug(f"{contact.display_name} ({contact.uin}) offline")

        def on_typing(uin: str, is_typing: bool):
            logging.debug(f"Typing from {uin}: {'начал' if is_typing else 'закончил'}")
            asyncio.create_task(handle_typing(client, uin, is_typing))

        def on_error(exc: Exception):
            nonlocal last_error
            last_error = exc
            logging.error(f"ICQ error: {exc}")

        client.on_connected       = on_connected
        client.on_disconnected    = on_disconnected
        client.on_roster          = on_roster
        client.on_contact_online  = on_contact_online
        client.on_contact_offline = on_contact_offline
        client.on_message         = on_message
        client.on_typing          = on_typing
        client.on_error           = on_error

        logging.info("Подключение к серверу ICQ…")
        await client.run()

        for task in list(active_tasks):
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        if isinstance(last_error, AuthError):
            logging.error(f"Ошибка аутентификации: {last_error}")
            break

        logging.error(f"Соединение потеряно ({last_error}). Повтор через {delay} сек…")
        await asyncio.sleep(delay)
        delay = min(delay * 2, RECONNECT_DELAY_MAX)


if __name__ == "__main__":
    asyncio.run(main())