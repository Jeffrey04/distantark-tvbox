import asyncio
import subprocess
from queue import Queue
from threading import Event

import structlog

logger = structlog.getLogger(__name__)


async def playback(queue: Queue) -> None:
    logger.info("VIDEO: Starting video player")
    process = await asyncio.create_subprocess_exec(
        "mpv",
        *["-", "--fullscreen"],
        stdin=subprocess.PIPE,
        # stdout=subprocess.PIPE,
        # stderr=subprocess.PIPE,
    )

    while data := await asyncio.to_thread(queue.get):
        process.stdin.write(data)  # type: ignore


async def run(exit_event: Event, queue: Queue) -> None:
    logger.info("VIDEO: Video playback process is running")

    asyncio.create_task(playback(queue))

    await asyncio.to_thread(exit_event.wait)
