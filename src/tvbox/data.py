import asyncio
import subprocess
from asyncio.subprocess import Process
from functools import partial
from queue import Queue
from threading import Event

import httpx
import structlog

logger = structlog.getLogger(__name__)


async def write_input(
    client: httpx.AsyncClient, video_link: str, process: asyncio.subprocess.Process
) -> None:
    async with client.stream("GET", video_link) as response:
        logger.info("DATA: Streaming video to queue", link=video_link)
        async for chunk in response.aiter_raw(1024):
            process.stdin.write(chunk)  # type: ignore


async def video_send(queue: Queue, client: httpx.AsyncClient, video_link: str) -> None:
    logger.info("DATA: Fetching video from link", link=video_link)
    process = await asyncio.create_subprocess_shell(
        " ".join(
            [
                "ffmpeg",
                "-hwaccel",
                "cuda",
                "-i",
                "pipe:0",
                "-c:v",
                "libx264",
                "-b:v",
                "1.5M",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-f",
                "mpegts",
                "-y",
                "pipe:1",
            ]
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    asyncio.create_task(write_input(client, video_link, process))

    while not process.stdout.at_eof():  # type: ignore
        await asyncio.to_thread(partial(queue.put, await process.stdout.read(1024)))  # type: ignore


async def data_poll(queue: Queue):
    sent_videos = []

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                "https://replicantlife.com/distribution/station_feed"
            )

            for video in reversed(response.json()):
                if video["link"] not in sent_videos:
                    sent_videos.append(video["link"])
                    await video_send(queue, client, video["link"])

            await asyncio.sleep(30 * 60)


async def run(event_exit: Event, queue: Queue) -> None:
    logger.info("DATA: Data fetching process is running")

    asyncio.create_task(data_poll(queue))

    await asyncio.to_thread(event_exit.wait)
    logger.info("DATA: Data fetching process is exiting")
