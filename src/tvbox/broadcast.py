import asyncio
from functools import partial
from queue import Empty, Queue
from threading import Event

import structlog

logger = structlog.getLogger(__name__)


async def run(
    exit_event: Event, queue_data: Queue, queue_play: Queue, queue_stream: Queue
) -> None:
    logger.info("BROADCAST: Data broadcasting process is running")
    asyncio.create_task(queue_broadcast(queue_data, queue_play, queue_stream))

    await asyncio.to_thread(exit_event.wait)
    logger.info("BROADCAST: Data broadcasting process is exiting")


async def queue_broadcast(queue_data: Queue, *queues: Queue) -> None:
    while True:
        try:
            logger.debug(
                "BROADCAST: Data broadcasting process is waiting for video data"
            )
            data = await asyncio.to_thread(partial(queue_data.get, timeout=5))

            async with asyncio.TaskGroup() as tg:
                logger.debug(
                    "BROADCAST: Data broadcasting process is broadcasting video data to play and stream queue"
                )
                for queue in queues:
                    tg.create_task(asyncio.to_thread(partial(queue.put, data)))

        except Empty:
            continue
