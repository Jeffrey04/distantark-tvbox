import asyncio
import subprocess
from os import environ
from queue import Queue
from threading import Event

import structlog

logger = structlog.getLogger(__name__)


async def stream(queue: Queue) -> None:
    logger.info("STREAM: Starting video streaming")
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hwaccel",
        "cuda",
        "-re",
        "-y",
        "-i",
        "pipe:0",
        "-c:a",
        "copy",
        "-ac",
        "1",
        "-ar",
        "44100",
        "-b:a",
        "96k",
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-tune",
        "zerolatency",
        "-f",
        "flv",
        "-maxrate",
        "2000k",
        "-preset",
        "veryfast",
        "-g",
        "60",
        f"rtmp://live-fra.twitch.tv/app/{environ['TWITCH_KEY']}",
        stdin=subprocess.PIPE,
    )

    assert isinstance(process.stdin, asyncio.StreamWriter)

    while data := await asyncio.to_thread(queue.get):
        process.stdin.write(data)
        await process.stdin.drain()

    process.stdin.close()
    await process.stdin.wait_closed()

    logger.info("STREAM: Done streaming video")


async def run(exit_event: Event, queue: Queue) -> None:
    logger.info("STREAM: Video streaming process is running")

    asyncio.create_task(stream(queue))

    await asyncio.to_thread(exit_event.wait)
