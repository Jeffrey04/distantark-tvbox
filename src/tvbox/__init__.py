import signal
from collections.abc import Callable, Coroutine
from concurrent.futures import ProcessPoolExecutor
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


def shutdown_handler(_signum, _frame, exit_event: Event) -> None:
    logger.info("MAIN: Sending exit event to all tasks in pool")
    exit_event.set()


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
            signal.signal(s, partial(shutdown_handler, exit_event=event_exit))

        logger.info("MAIN: Spawning DATA fetching process")
        executor.submit(process_run, data_run, event_exit, queue_data)

        logger.info("MAIN: Spwawing DATA broadcasting process")
        executor.submit(
            process_run, broadcast_run, event_exit, queue_data, queue_play, queue_stream
        )

        logger.info("MAIN: Spawning STREAM video streaming process")
        executor.submit(process_run, stream_run, event_exit, queue_stream)

        logger.info("MAIN: Spawning VIDEO player process")
        executor.submit(process_run, video_run, event_exit, queue_play)


def main() -> None:
    typer.run(start)
