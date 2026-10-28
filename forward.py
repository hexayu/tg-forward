# Forwards new posts from a public Telegram channel to your Saved Messages.
# Runs on a schedule (GitHub Actions). No state file needed: it checks your
# Saved Messages for the last post it already forwarded and continues from there.
import asyncio
import os

from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
CHANNEL = os.environ.get("TG_CHANNEL", "grihasthtantra")
MAX_PER_RUN = 100


async def main():
    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        channel = await client.get_entity(CHANNEL)

        # Find the newest channel post already forwarded to Saved Messages.
        last_id = 0
        async for m in client.iter_messages("me", limit=300):
            f = m.fwd_from
            if (
                f
                and f.channel_post
                and getattr(f.from_id, "channel_id", None) == channel.id
            ):
                last_id = max(last_id, f.channel_post)

        # First run: forward only the latest post to set a starting point.
        if last_id == 0:
            latest = await client.get_messages(channel, limit=1)
            if latest:
                await latest[0].forward_to("me")
                print(f"Started from post {latest[0].id}")
            return

        new_posts = []
        async for m in client.iter_messages(channel, min_id=last_id, reverse=True):
            new_posts.append(m)
            if len(new_posts) >= MAX_PER_RUN:
                break

        for m in new_posts:
            try:
                await m.forward_to("me")
                print(f"Forwarded post {m.id}")
            except Exception as e:  # service messages, protected posts, etc.
                print(f"Skipped post {m.id}: {type(e)._name_}")
            await asyncio.sleep(1)

        if not new_posts:
            print("No new posts")


asyncio.run(main())
