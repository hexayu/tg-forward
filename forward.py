# Forwards new posts from a public Telegram channel to your Saved Messages.
# Runs on a schedule (GitHub Actions). No state file needed: it checks your
# Saved Messages for the last post it already forwarded and continues from there.
# It also checks whether the channel is live, so the workflow can start the
# live recorder (record_live.py).
import asyncio
import os

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.phone import GetGroupCallRequest
from telethon.tl.types import GroupCallDiscarded

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
CHANNEL = os.environ.get("TG_CHANNEL", "grihasthtantra")
MAX_PER_RUN = 100
BATCH = 20  # posts forwarded per request (fewer requests = fewer rate limits)


async def forward_batch(client, channel, ids):
    """Forward a list of post IDs; on failure, retry one by one."""
    try:
        await client.forward_messages("me", ids, from_peer=channel)
        return len(ids)
    except errors.FloodWaitError:
        raise
    except Exception as e:
        print(f"Batch failed ({e}), trying one by one", flush=True)
    sent = 0
    for i in ids:
        try:
            await client.forward_messages("me", i, from_peer=channel)
            sent += 1
        except errors.FloodWaitError:
            raise
        except Exception as e:
            print(f"Skipped post {i}: {e}", flush=True)
        await asyncio.sleep(1)
    return sent


async def forward_new_posts(client, channel):
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
            await forward_batch(client, channel, [latest[0].id])
            print(f"Started from post {latest[0].id}", flush=True)
        return

    ids = []
    async for m in client.iter_messages(channel, min_id=last_id, reverse=True):
        if m.action is None:  # skip service messages (pins, title changes)
            ids.append(m.id)
        if len(ids) >= MAX_PER_RUN:
            break

    if not ids:
        print("No new posts", flush=True)
        return

    total = 0
    try:
        for start in range(0, len(ids), BATCH):
            chunk = ids[start:start + BATCH]
            total += await forward_batch(client, channel, chunk)
            print(f"Forwarded posts {chunk[0]}-{chunk[-1]}", flush=True)
            await asyncio.sleep(3)
    except errors.FloodWaitError as e:
        # Telegram asked us to slow down. Stop cleanly; the next run
        # continues from the last forwarded post, so nothing is lost.
        print(f"Rate limited for {e.seconds}s, will continue next run", flush=True)

    print(f"Done: {total} post(s) forwarded", flush=True)


async def report_live_status(client, channel):
    """Tell the workflow whether the channel is live right now."""
    live = False
    try:
        call = (await client(GetFullChannelRequest(channel))).full_chat.call
        if call is not None:
            try:
                info = (await client(GetGroupCallRequest(call=call, limit=1))).call
                live = (not isinstance(info, GroupCallDiscarded)
                        and not getattr(info, "schedule_date", None))
            except Exception:
                live = True  # a live exists but its details couldn't be read
    except Exception as e:
        print(f"Live check failed: {e}", flush=True)
    print("Channel is LIVE" if live else "Channel is not live", flush=True)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as f:
            f.write(f"live={'true' if live else 'false'}\n")


async def main():
    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        client.flood_sleep_threshold = 120  # wait out short rate limits automatically
        channel = await client.get_entity(CHANNEL)
        try:
            await forward_new_posts(client, channel)
        finally:
            await report_live_status(client, channel)


asyncio.run(main())
