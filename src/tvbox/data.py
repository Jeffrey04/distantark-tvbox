import asyncio
import subprocess
from functools import partial
from queue import Queue
from threading import Event

import httpx
import structlog

logger = structlog.getLogger(__name__)


async def write_input(
    client: httpx.AsyncClient, video_link: str, process: asyncio.subprocess.Process
) -> None:
    assert isinstance(process.stdin, asyncio.StreamWriter)

    try:
        async with client.stream("GET", video_link) as response:
            logger.info("DATA: Streaming video to queue", link=video_link)
            async for chunk in response.aiter_raw(1024):
                process.stdin.write(chunk)
                await process.stdin.drain()

            process.stdin.close()
            await process.stdin.wait_closed()

        logger.info("DATA: Done downloading video to ffmpeg")

    except Exception as e:
        logger.error(
            "Encountered error in processing video link, skipping",
            video_link=video_link,
        )
        logger.exception(e)


async def video_send(queue: Queue, client: httpx.AsyncClient, video_link: str) -> None:
    logger.info("DATA: Fetching video from link", link=video_link)
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hwaccel",
        "cuda",
        "-i",
        "pipe:0",
        "-c:v",
        "h264_nvenc",
        # "libx264",
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
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    asyncio.create_task(write_input(client, video_link, process))

    assert isinstance(process.stdout, asyncio.StreamReader)

    while True:
        try:
            chunk = await asyncio.wait_for(process.stdout.read(1024), 5)

        except asyncio.TimeoutError:
            break

        if not chunk:
            break
        else:
            await asyncio.to_thread(partial(queue.put, chunk))

    logger.info("DATA: Done sending video to queue")


async def data_poll(queue: Queue):
    sent_videos = []

    async with httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=99)
    ) as client:
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
