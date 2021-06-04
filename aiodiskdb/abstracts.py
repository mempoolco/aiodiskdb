import abc
import asyncio
import time

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running


class AsyncLockable(metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._reads_count = 0

    def _incr_read(self):
        self._reads_count += 1

    def _decr_read(self):
        self._reads_count -= 1


class AsyncRunnable(metaclass=abc.ABCMeta):
    def __init__(self, stop_timeout=60):
        super().__init__()
        self._running = False
        self._error = False
        self._do_stop = False
        self._stop_timeout = stop_timeout

    @abc.abstractmethod
    async def _run_loop(self):
        pass  # pragma: no cover

    @abc.abstractmethod
    def _teardown(self):
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
                    await self._teardown()
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
        if self._do_stop:
            raise exceptions.FailedToStopException('Stop already hit')
        timeout = 60
        stop_at = time.time()
        self._do_stop = True
        while stop_at - time.time() < timeout:
            if not self._running:
                return True
            await asyncio.sleep(0.1)
        raise exceptions.FailedToStopException(f'Loop is still running after {self._stop_timeout} seconds')
