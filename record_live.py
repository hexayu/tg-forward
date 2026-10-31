# Records a Telegram channel live stream (audio + video) and sends it to your
# Saved Messages as an MP3 and an MP4.
#
# Started by the "Record Telegram live" workflow when forward.py sees that the
# channel is live. It uses its OWN Telegram login (secret TG_SESSION_LIVE),
# because Telegram cancels a login that is used from two servers at once.
#
# How it works: it joins the live as a silent listener, receives the raw audio
# and video, and lines both up against the clock (gaps become silence or a
# black picture) so sound and picture stay in sync. When the live ends it builds
# the files and uploads them. Lives longer than ~5 h are saved in parts,
# because GitHub stops a job after 6 hours.
import asyncio
import glob
import math
import os
import queue
import re
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

CHANNEL = os.environ.get("TG_CHANNEL", "grihasthtantra")
MAX_MINUTES = float(os.environ.get("MAX_RECORD_MINUTES", "315"))
FPS = int(os.environ.get("VIDEO_FPS", "25"))
LONG_SIDE = int(os.environ.get("VIDEO_LONG_SIDE", "1280"))  # 1280 = 720p
CRF = os.environ.get("VIDEO_CRF", "28")
MAXRATE_K = int(os.environ.get("VIDEO_MAXRATE_K", "1500"))
PART_LIMIT = int(float(os.environ.get("PART_LIMIT_MB", "1900")) * 1024 * 1024)
WORKDIR = Path(os.environ.get("RECORD_DIR", "recording"))
TZ = ZoneInfo("Asia/Kolkata")

AUDIO_RATE = 48000
AUDIO_CHANNELS = 2
SAMPLE_BYTES = 2 * AUDIO_CHANNELS  # one 16-bit stereo sample
GAP_FILL_AFTER = 0.5  # audio this far behind the clock = a real gap -> silence
AHEAD_SOFT = 0.3      # audio this far ahead -> drop quiet 10 ms chunks to catch up
AHEAD_HARD = 2.0      # audio this far ahead -> drop any chunk
QUIET_RMS = 300
SOURCE_ACTIVE = 2.0   # a video source counts as "on" if it sent a frame this recently
BLACK_AFTER = 5.0     # no video for this long -> black picture
CHECK_EVERY = 20      # seconds between "is it still live?" checks
STALL_REJOIN = 180    # no audio/video at all for this long -> rejoin
REJOIN_BASE = 20      # first wait before rejoining; doubles each time, max 5 min


def log(msg):
    print(datetime.now(TZ).strftime("%H:%M:%S"), msg, flush=True)


def ffmpeg(*args):
    return ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *args]


def run(cmd):
    subprocess.run(cmd, check=True, stdin=subprocess.DEVNULL)


def probe_duration(path):
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True, text=True, stdin=subprocess.DEVNULL,
    ).stdout.strip()
    try:
        return float(out)
    except ValueError:
        return 0.0


def fmt_len(seconds):
    m = int(seconds // 60)
    return f"{m // 60}h {m % 60:02d}m" if m >= 60 else f"{m}m {int(seconds % 60):02d}s"


def safe_name(text):
    text = re.sub(r'[\\/:*?"<>|\n\r\t]+', " ", text).strip()
    return text[:80] or "live"


class Timeline:
    """Shared clock for audio and video: 0 = the first frame received."""

    def __init__(self):
        self.origin = None

    def mark(self):
        now = time.monotonic()
        if self.origin is None:
            self.origin = now
        return now - self.origin

    def now(self):
        return None if self.origin is None else time.monotonic() - self.origin


# ---------------------------------------------------------------- audio
def mix(chunks):
    """Mix 10 ms PCM chunks from several speakers into one."""
    chunks = [c for c in chunks if c]
    if not chunks:
        return b""
    if len(chunks) == 1:
        c = chunks[0]
        return c[: len(c) - len(c) % SAMPLE_BYTES]
    n = min(len(c) for c in chunks) // SAMPLE_BYTES * AUDIO_CHANNELS
    total = np.zeros(n, dtype=np.int32)
    for c in chunks:
        total += np.frombuffer(c, dtype=np.int16, count=n)
    np.clip(total, -32768, 32767, out=total)
    return total.astype(np.int16).tobytes()


def rms(data):
    a = np.frombuffer(data, dtype=np.int16).astype(np.float32)
    return float(np.sqrt(np.mean(a * a))) if a.size else 0.0


class AudioTrack:
    """Writes incoming audio to MP3 (to send) and AAC (for the MP4) as one
    continuous track that follows the clock."""

    def __init__(self, workdir, title):
        self.mp3 = workdir / "audio.mp3"
        self.aac = workdir / "audio.aac"
        self.pos = 0        # samples queued so far
        self.received = 0   # chunks received
        self.dropped = 0
        self.filled = 0.0   # seconds of silence inserted
        self.failed = False
        self.proc = subprocess.Popen(
            ffmpeg("-f", "s16le", "-ar", str(AUDIO_RATE), "-ac", str(AUDIO_CHANNELS),
                   "-i", "pipe:0",
                   "-map", "0:a", "-c:a", "libmp3lame", "-b:a", "128k",
                   "-metadata", f"title={title}", str(self.mp3),
                   "-map", "0:a", "-c:a", "aac", "-b:a", "128k", "-f", "adts", str(self.aac)),
            stdin=subprocess.PIPE,
        )
        self.q = queue.Queue()
        self.thread = threading.Thread(target=self._writer, daemon=True)
        self.thread.start()

    def _writer(self):
        silence = bytes(AUDIO_RATE * SAMPLE_BYTES)  # 1 second
        while (item := self.q.get()) is not None:
            if self.failed:
                continue
            try:
                if isinstance(item, int):  # that many samples of silence
                    left = item * SAMPLE_BYTES
                    while left > 0:
                        k = min(left, len(silence))
                        self.proc.stdin.write(silence[:k])
                        left -= k
                else:
                    self.proc.stdin.write(item)
            except (BrokenPipeError, OSError, ValueError):
                self.failed = True
                log("Audio encoder stopped unexpectedly")

    def _silence(self, samples):
        if samples > 0:
            self.q.put(samples)
            self.pos += samples
            self.filled += samples / AUDIO_RATE

    def add(self, chunks, t):
        data = mix(chunks)
        n = len(data) // SAMPLE_BYTES
        if n == 0:
            return
        self.received += 1
        behind = t - self.pos / AUDIO_RATE
        if behind > GAP_FILL_AFTER:
            self._silence(int(round(behind * AUDIO_RATE)))
        elif -behind > AHEAD_HARD or (-behind > AHEAD_SOFT and rms(data) < QUIET_RMS):
            self.dropped += 1
            return
        self.q.put(data)
        self.pos += n

    def finish(self, duration):
        self._silence(int(duration * AUDIO_RATE) - self.pos)
        self.q.put(None)
        self.thread.join()
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        self.proc.wait()


# ---------------------------------------------------------------- video
ROTATE_FILTER = {90: "transpose=1,", 180: "hflip,vflip,", 270: "transpose=2,"}


def i420_size(w, h):
    return w * h + 2 * ((w + 1) // 2) * ((h + 1) // 2)


def canvas_for(w, h, rotation):
    """Output size: same shape as the first picture, long side = LONG_SIDE."""
    if rotation in (90, 270):
        w, h = h, w
    scale = LONG_SIDE / max(w, h)
    return max(2, round(w * scale / 2) * 2), max(2, round(h * scale / 2) * 2)


class Segment:
    """One ffmpeg encoder for a run of frames with the same size/rotation."""

    def __init__(self, path, key, canvas):
        w, h, rotation = key
        cw, ch = canvas
        vf = (f"{ROTATE_FILTER.get(rotation, '')}"
              f"scale={cw}:{ch}:force_original_aspect_ratio=decrease:force_divisible_by=2,"
              f"pad={cw}:{ch}:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p")
        self.path, self.key, self.frames, self.broken = path, key, 0, False
        self.proc = subprocess.Popen(
            ffmpeg("-f", "rawvideo", "-pix_fmt", "yuv420p", "-s", f"{w}x{h}",
                   "-r", str(FPS), "-i", "pipe:0", "-vf", vf,
                   "-c:v", "libx264", "-preset", "veryfast", "-crf", CRF,
                   "-maxrate", f"{MAXRATE_K}k", "-bufsize", f"{MAXRATE_K * 2}k",
                   "-g", str(FPS * 2), "-keyint_min", str(FPS * 2), "-sc_threshold", "0",
                   "-f", "mpegts", str(path)),
            stdin=subprocess.PIPE,
        )

    def write(self, data):
        try:
            self.proc.stdin.write(data)
            self.frames += 1
        except (BrokenPipeError, OSError, ValueError):
            self.broken = True

    def close(self):
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        self.proc.wait()


class VideoTrack:
    """Turns incoming frames (any size, any rate, with gaps) into a steady
    FPS video that follows the clock."""

    def __init__(self, workdir):
        self.workdir = workdir
        self.lock = threading.Lock()
        self.sources = {}  # ssrc -> [is_screen, w, h, rotation, data, last_t]
        self.current = None
        self.first_t = None
        self.canvas = None
        self.written = 0
        self.received = 0
        self.bad = 0
        self.seg = None
        self.segments = []
        self._black = {}
        self._stop = threading.Event()
        self._thread = None

    def add(self, ssrc, is_screen, data, w, h, rotation, t):
        if w <= 0 or h <= 0 or len(data) != i420_size(w, h):
            self.bad += 1
            return
        with self.lock:
            if self.first_t is None:
                self.first_t = t
                self.canvas = canvas_for(w, h, rotation)
                log(f"Video started ({w}x{h}), saving at {self.canvas[0]}x{self.canvas[1]}")
            self.sources[ssrc] = [is_screen, w, h, rotation, data, t]
            self.received += 1

    def _pick(self, t):
        """Screen share wins over camera; otherwise stay on the current source."""
        active = {s: v for s, v in self.sources.items() if t - v[5] <= SOURCE_ACTIVE}
        if active:
            cur = active.get(self.current)
            screens = [s for s, v in active.items() if v[0]]
            if cur is None or (screens and not cur[0]):
                self.current = screens[0] if screens else max(active, key=lambda s: active[s][5])
            return active[self.current]
        last = self.sources.get(self.current)
        return last if last and t - last[5] <= BLACK_AFTER else None

    def _black_frame(self, w, h):
        if (w, h) not in self._black:
            self._black[(w, h)] = bytes([16]) * (w * h) + bytes([128]) * (i420_size(w, h) - w * h)
        return self._black[(w, h)]

    def _write(self, src):
        if src is None:
            key = self.seg.key if self.seg else (self.canvas[0], self.canvas[1], 0)
            data = self._black_frame(key[0], key[1])
        else:
            key, data = (src[1], src[2], src[3]), src[4]
        if self.seg is None or self.seg.key != key or self.seg.broken:
            if self.seg:
                self.seg.close()
            path = self.workdir / f"video_{len(self.segments):03d}.ts"
            self.seg = Segment(path, key, self.canvas)
            self.segments.append(path)
        self.seg.write(data)

    def tick(self, t):
        """Write every frame that is due up to clock time t (frame i shows time i/FPS)."""
        while self.written <= int(t * FPS):
            with self.lock:
                if self.first_t is None:
                    return
                src = None if self.written / FPS < self.first_t else self._pick(t)
            self._write(src)
            self.written += 1

    def start(self, timeline):
        def loop():
            while not self._stop.wait(0.5 / FPS):
                t = timeline.now()
                if t is not None:
                    try:
                        self.tick(t)
                    except Exception as e:  # keep recording audio even if video fails
                        log(f"Video error: {e!r}")

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def finish(self, duration):
        self._stop.set()
        if self._thread:
            self._thread.join()
        self.tick(duration)
        if self.seg:
            self.seg.close()


# ---------------------------------------------------------------- output files
def split_video(path):
    """Split an MP4 into parts under Telegram's size limit (no re-encoding)."""
    size = path.stat().st_size
    if size <= PART_LIMIT:
        return [path]
    parts = math.ceil(size / (PART_LIMIT * 0.85))
    pattern = path.with_name(f"{path.stem} - part %d.mp4")
    run(ffmpeg("-i", str(path), "-map", "0", "-c", "copy", "-f", "segment",
               "-segment_time", f"{probe_duration(path) / parts:.3f}",
               "-reset_timestamps", "1", "-segment_start_number", "1",
               "-segment_format_options", "movflags=+faststart", str(pattern)))
    path.unlink()
    found = path.parent.glob(f"{glob.escape(path.stem)} - part *.mp4")
    result = []
    for p in sorted(found, key=lambda p: int(p.stem.rsplit(" ", 1)[-1])):
        result += split_video(p)
    return result


def build_outputs(audio, video, duration, name):
    files = []
    if audio.received and audio.mp3.exists():
        mp3 = audio.mp3.with_name(f"{name}.mp3")
        audio.mp3.rename(mp3)
        files.append(("audio", mp3))
    if video.received and video.segments:
        listing = video.workdir / "segments.txt"
        listing.write_text("".join(f"file '{p.name}'\n" for p in video.segments))
        mp4 = video.workdir / f"{name}.mp4"
        concat = ["-f", "concat", "-safe", "0", "-i", str(listing)]
        tail = ["-t", f"{duration:.3f}", "-movflags", "+faststart", str(mp4)]
        try:
            run(ffmpeg(*concat, "-i", str(audio.aac), "-map", "0:v:0", "-map", "1:a:0",
                       "-c", "copy", "-bsf:a", "aac_adtstoasc", *tail))
        except subprocess.CalledProcessError:
            log("Could not add sound to the video, saving the video without it")
            run(ffmpeg(*concat, "-map", "0:v:0", "-c", "copy", *tail))
        for p in video.segments:
            p.unlink(missing_ok=True)
        files += [("video", p) for p in split_video(mp4)]
    return files


def make_thumb(path):
    thumb = path.with_suffix(".jpg")
    at = min(10.0, probe_duration(path) / 2)
    subprocess.run(
        ffmpeg("-ss", f"{at:.2f}", "-i", str(path), "-frames:v", "1",
               "-vf", "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)'",
               "-q:v", "5", str(thumb)),
        stdin=subprocess.DEVNULL,
    )
    return str(thumb) if thumb.exists() else None


# ---------------------------------------------------------------- Telegram
async def live_state(client, channel):
    """Returns ("live" | "scheduled" | "none", live title)."""
    from telethon.tl.functions.channels import GetFullChannelRequest
    from telethon.tl.functions.phone import GetGroupCallRequest
    from telethon.tl.types import GroupCallDiscarded

    full = await client(GetFullChannelRequest(channel))
    call = full.full_chat.call
    if call is None:
        return "none", None
    try:
        info = (await client(GetGroupCallRequest(call=call, limit=1))).call
    except Exception:
        return "live", None
    if isinstance(info, GroupCallDiscarded):
        return "none", None
    if getattr(info, "schedule_date", None):
        return "scheduled", info.title
    return "live", info.title


async def send_files(client, files, video_canvas, title, channel_title, started, ended, duration):
    from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

    when = f"{started:%d %b %Y, %I:%M %p} – {ended:%I:%M %p} IST · {fmt_len(duration)}"
    heading = f"🔴 Live: {channel_title}" + (f"\n{title}" if title and title != channel_title else "")
    videos = sum(1 for kind, _ in files if kind == "video")
    n = 0
    for kind, path in files:
        length = probe_duration(path)
        caption = f"{heading}\n{when}"
        if kind == "audio":
            attrs = [DocumentAttributeAudio(duration=int(round(length)), title=title or channel_title,
                                            performer=channel_title)]
            thumb = None
        else:
            n += 1
            if videos > 1:
                caption += f"\nVideo part {n} of {videos}"
            attrs = [DocumentAttributeVideo(duration=length, w=video_canvas[0], h=video_canvas[1],
                                            supports_streaming=True)]
            thumb = make_thumb(path)
        size_mb = path.stat().st_size / 1048576
        log(f"Uploading {path.name} ({size_mb:.0f} MB)")
        last = [0]

        def progress(sent, total):
            pct = int(sent * 100 / total) if total else 100
            if pct >= last[0] + 20:
                last[0] = pct
                log(f"  {pct}%")

        for attempt in range(1, 4):  # big uploads can hit a network blip: retry
            try:
                await client.send_file("me", str(path), caption=caption, attributes=attrs,
                                       thumb=thumb, supports_streaming=(kind == "video"),
                                       progress_callback=progress)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                log(f"Upload failed ({type(e).__name__}: {e}), retrying in 30s")
                last[0] = 0
                await asyncio.sleep(30)
    log("All files sent to Saved Messages")


async def wait_for_end(client, channel, deadline):
    while time.monotonic() < deadline:
        await asyncio.sleep(60)
        try:
            if (await live_state(client, channel))[0] == "none":
                return
        except Exception:
            pass


async def record(client):
    from telethon import utils
    from pytgcalls import PyTgCalls, filters
    from pytgcalls.types import ChatUpdate, Device, Direction, GroupCallConfig, RecordStream
    from pytgcalls.types.raw import AudioParameters
    from ntgcalls import VideoRotation

    rotation = {VideoRotation.VIDEO_ROTATION_90: 90, VideoRotation.VIDEO_ROTATION_180: 180,
                VideoRotation.VIDEO_ROTATION_270: 270}
    deadline = time.monotonic() + MAX_MINUTES * 60
    channel = await client.get_entity(CHANNEL)
    chat_id = utils.get_peer_id(channel)
    state, call_title = await live_state(client, channel)
    if state == "none":
        log(f"@{CHANNEL} is not live. Nothing to record.")
        return 0
    if state == "scheduled":
        log("A live is scheduled; waiting for it to start")
    title = call_title or channel.title
    started = datetime.now(TZ)
    log(f"Recording live on {channel.title}")

    WORKDIR.mkdir(parents=True, exist_ok=True)
    timeline = Timeline()
    audio = AudioTrack(WORKDIR, title)
    video = VideoTrack(WORKDIR)
    video.start(timeline)
    last_media = [time.monotonic()]
    wake = asyncio.Event()
    reason = "time limit reached"
    joins = 0

    try:
        calls = PyTgCalls(client)

        @calls.on_update(filters.stream_frame(Direction.INCOMING))
        async def on_frames(_, update):
            if update.chat_id != chat_id or not update.frames:
                return
            t = timeline.mark()
            last_media[0] = time.monotonic()
            if update.device in (Device.MICROPHONE, Device.SPEAKER):
                audio.add([f.frame for f in update.frames], t)
            elif update.device in (Device.CAMERA, Device.SCREEN):
                for f in update.frames:
                    video.add(f.ssrc, update.device == Device.SCREEN, f.frame, f.info.width,
                              f.info.height, rotation.get(f.info.rotation, 0), t)

        @calls.on_update(filters.chat_update(ChatUpdate.Status.LEFT_CALL))
        async def on_left(_, update):
            if update.chat_id == chat_id:
                wake.set()

        await calls.start()
        drops = 0
        joined_before = False
        next_join = 0.0
        healthy_since = None
        while time.monotonic() < deadline:
            now = time.monotonic()
            in_call = chat_id in await calls.calls
            if in_call and now - last_media[0] > STALL_REJOIN:
                log("No sound or picture for 3 minutes, rejoining")
                try:
                    await calls.leave_call(chat_id)
                except Exception:
                    pass
                in_call = False
            if not in_call and healthy_since is not None:
                # We were in the live and got dropped. Wait a little longer
                # after every drop (20 s, 40 s, ... up to 5 min) so we never
                # fight with you if you open the live on your own phone.
                healthy_since = None
                drops += 1
                next_join = now + min(300, REJOIN_BASE * 2 ** (drops - 1))
                if state == "live":
                    log(f"Disconnected from the live, trying again in {int(next_join - now)}s")
            if in_call:
                if healthy_since and now - healthy_since > 600:
                    drops = 0  # stable for 10 minutes: reset the back-off
            elif state == "live" and now >= next_join:
                joins += 1
                try:
                    await asyncio.wait_for(calls.record(
                        chat_id,
                        RecordStream(audio=True,
                                     audio_parameters=AudioParameters(AUDIO_RATE, AUDIO_CHANNELS),
                                     camera=True, screen=True),
                        GroupCallConfig(auto_start=False),  # never start a live ourselves
                    ), 90)
                    log("Rejoined the live" if joined_before else "Joined the live")
                    joined_before = True
                    last_media[0] = healthy_since = time.monotonic()
                except Exception as e:
                    log(f"Could not join the live: {type(e).__name__}: {e}")
                    drops += 1
                    next_join = time.monotonic() + min(300, REJOIN_BASE * 2 ** (drops - 1))

            wake.clear()
            try:
                await asyncio.wait_for(wake.wait(), CHECK_EVERY)
            except asyncio.TimeoutError:
                pass
            try:
                state, _ = await live_state(client, channel)
            except Exception as e:
                log(f"Live check failed ({e}), will retry")
                continue
            if state == "none":
                reason = "live ended"
                break
        try:
            await calls.leave_call(chat_id)
        except Exception:
            pass
    except Exception:
        traceback.print_exc()
        reason = "error"

    ended = datetime.now(TZ)
    duration = timeline.now() or 0.0
    video.finish(duration)
    audio.finish(duration)
    log(f"Stopped ({reason}): {fmt_len(duration)} recorded, {audio.received} audio chunks, "
        f"{video.received} video frames ({video.bad} unusable), {audio.filled:.0f}s silence added, "
        f"{audio.dropped} audio chunks dropped")

    if not audio.received and not video.received:
        if joins and (ended - started).total_seconds() > 120:  # tried, and failed, for 2+ min
            await client.send_message(
                "me", f"⚠️ {channel.title} went live at {started:%I:%M %p}, but the recorder "
                      "couldn't capture it. The GitHub run log has the details.")
        if reason == "error":
            await wait_for_end(client, channel, deadline)  # one failed run per live, not one per minute
        return 1

    name = safe_name(f"{started:%Y-%m-%d %H%M} {channel.title}")
    files = build_outputs(audio, video, duration, name)
    await send_files(client, files, video.canvas, title, channel.title, started, ended, duration)
    return 1 if reason == "error" else 0


async def main():
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    session = os.environ.get("TG_SESSION_LIVE", "").strip()
    if not session:
        log("Missing secret TG_SESSION_LIVE (a second login just for the recorder).")
        return 1
    client = TelegramClient(StringSession(session), int(os.environ["TG_API_ID"]),
                            os.environ["TG_API_HASH"])
    await client.connect()
    try:
        if not await client.is_user_authorized():
            log("TG_SESSION_LIVE is not a working login. Create it again with login.py.")
            return 1
        return await record(client)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
