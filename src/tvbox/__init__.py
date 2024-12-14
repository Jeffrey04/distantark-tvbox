import signal
from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
from multiprocessing import Manager
from threading import Event
from typing import Any

import structlog
import typer
import uvloop

from tvbox.broadcast import run as broadcast_run
from tvbox.data import run as data_run
from tvbox.stream import run as stream_run
from tvbox.video import run as video_run

logger = structlog.getLogger(__name__)

def done_handler(
    future: Future,
    name: str,
    event_exit: Event,
) -> None:
    logger.info(
        "MAIN: Task is done, prompting others to quit",
        name=name,
        future=future,
    )

    if future.exception() is not None:
        logger.exception(future.exception())

    shutdown_handler(None, None, event_exit)


def shutdown_handler(_signum, _frame, event_exit: Event) -> None:
    logger.info("MAIN: Sending exit event to all tasks in pool")
    event_exit.set()


def process_run(
    func: Callable[..., Coroutine[Any, Any, None]], *arguments: Any
) -> None:
    uvloop.run(func(*arguments))


def start() -> None:
    manager = Manager()
    event_exit = manager.Event()
    queue_data = manager.Queue()
    queue_play = manager.Queue()
    queue_stream = manager.Queue()

    with ProcessPoolExecutor(max_workers=5) as executor:
        for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            signal.signal(s, partial(shutdown_handler, event_exit=event_exit))

        logger.info("MAIN: Spawning DATA fetching process")
        executor.submit(
            process_run, data_run, event_exit, queue_data
        ).add_done_callback(partial(done_handler, name="DATA", event_exit=event_exit))

        logger.info("MAIN: Spwawing BROADCAST data broadcasting process")
        executor.submit(
            process_run,
            broadcast_run,
            event_exit,
            queue_data,
            queue_play,
            queue_stream,
        ).add_done_callback(
            partial(done_handler, name="BROADCAST", event_exit=event_exit)
        )

        logger.info("MAIN: Spawning STREAM video streaming process")
        executor.submit(
            process_run, stream_run, event_exit, queue_stream
        ).add_done_callback(partial(done_handler, name="STREAM", event_exit=event_exit))

        logger.info("MAIN: Spawning VIDEO player process")
        executor.submit(
            process_run, video_run, event_exit, queue_play
        ).add_done_callback(partial(done_handler, name="VIDEO", event_exit=event_exit))


def main() -> None:
    typer.run(start)
