"""Interactive Telegram login that can ask for the confirmation code again.

Pyrogram's built-in prompt cannot resend: the first code goes to the Telegram
app only, so a login is stuck when that message never shows up. Run:

    docker run --rm -it --user "$(id -u):$(id -g)" --env-file .env.docker \
        -v "$PWD:/app" --entrypoint python telegram-parser -m scripts.login
"""

import asyncio
from getpass import getpass

from pyrogram.errors import BadRequest, SessionPasswordNeeded

from parser.telegram import get_client


def describe(sent) -> str:
    next_type = sent.next_type.name if sent.next_type else "nothing"
    return (
        f"Code sent via {sent.type.name}. "
        f"Resend would use {next_type}, allowed after {sent.timeout} s."
    )


async def ask(prompt: str, reader=input) -> str:
    # Off the event loop, so Pyrogram keeps pinging while the user types.
    return (await asyncio.to_thread(reader, prompt)).strip()


async def check_password(client) -> None:
    while True:
        try:
            await client.check_password(await ask("2FA password: ", getpass))
            return
        except BadRequest as exc:
            print(exc)


async def login(client) -> None:
    phone = await ask("Phone number: ")
    sent = await client.send_code(phone)
    print(describe(sent))

    while True:
        code = await ask("Code (empty line = resend): ")
        try:
            if not code:
                sent = await client.resend_code(phone, sent.phone_code_hash)
                print(describe(sent))
                continue
            await client.sign_in(phone, sent.phone_code_hash, code)
        except SessionPasswordNeeded:
            await check_password(client)
        except BadRequest as exc:
            print(exc)
            continue
        return


async def main() -> None:
    client = get_client()
    if await client.connect():
        print("Already authorized, nothing to do.")
        await client.disconnect()
        return
    try:
        await login(client)
        print("Logged in, session saved.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
