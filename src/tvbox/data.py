import asyncio
import subprocess
from functools import partial
from queue import Queue
from threading import Event

import httpx
import structlog

logger = structlog.getLogger(__name__)


async def video_send(queue: Queue, client: httpx.AsyncClient, video_link: str) -> None:
    logger.info("DATA: Fetching video from link", link=video_link)
    response = await client.get(video_link)
    process = subprocess.run(
        [
            "ffmpeg",
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
        ],
        input=response.content,
        # stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    await asyncio.to_thread(partial(queue.put, process.stdout))
    # async with client.stream("GET", video_link) as response:
    #    logger.info("DATA: Streaming video to queue", link=video_link)
    #    async for chunk in response.aiter_raw(1024):
    #        (output, stderr) = process.communicate(chunk)

    #        await asyncio.to_thread(partial(queue.put, output))


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
