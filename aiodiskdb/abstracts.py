import abc
import asyncio
import time

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running


class AsyncLockable:
    def __init__(self):
        self._locks = dict(write=asyncio.Lock(), read=asyncio.Lock, reads_count=[])

    @property
    def _read_lock(self):
        return self._locks['read']

    @property
    def _write_lock(self):
        return self._locks['write']

    @property
    def _reads_count(self):
        return self._locks['reads_count']

    def _incr_read(self):
        self._locks['reads_count'] += 1

    def _decr_read(self):
        self._locks['reads_count'] -= 1


class AsyncRunnable(metaclass=abc.ABCMeta):
    def __init__(self, stop_timeout=60):
        self._running = False
        self._error = False
        self._do_stop = False
        self._stop_timeout = stop_timeout

    @abc.abstractmethod
    async def _run_loop(self):
        pass  # pragma: no cover

    @property
    def running(self):
        return self._running

    @ensure_running(False)
    async def run(self):
        """
        Must be launched before using the Database as a non blocking task.
        example:
        loop.create_task(instance.run())
        loop.run_until_complete()
        """
        if self._error:
            raise ValueError('error state')
        elif self._running:
            raise ValueError('already running')

        self._running = True
        while 1:
            try:
                if self._do_stop:
                    break
                await self._run_loop()
                await asyncio.sleep(0.005)
            except Exception as e:
                self._running = False
                self._error = e
                raise
        self._running = False

    @ensure_running(True)
    async def stop(self):
        timeout = 60
        stop_at = time.time()
        self._do_stop = True
        while time.time() - stop_at > timeout:
            if not self._running:
                break
            await asyncio.sleep(0.1)
        raise exceptions.FailedToStopException
