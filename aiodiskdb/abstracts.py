import abc
import asyncio
import time

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running, GracefulExit
from aiodiskdb.local_types import EventsHandlers


class AsyncLockable(metaclass=abc.ABCMeta):
    def __init__(self, *_, **__):
        super().__init__()
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._reads_count = 0

    def _incr_read(self):
        self._reads_count += 1

    def _decr_read(self):
        self._reads_count -= 1


class AsyncObservable(metaclass=abc.ABCMeta):
    def __init__(self, *_, **__):
        super().__init__(self, *_, **__)
        self._events = EventsHandlers()

    @property
    def events(self):
        return self._events


class AsyncRunnable(AsyncObservable, AsyncLockable, metaclass=abc.ABCMeta):
    def __init__(self, *_, stop_timeout=60, **__):
        super().__init__(*_, **__)
        self._running = False
        self._error = False
        self._do_stop = False
        self._stop_timeout = stop_timeout
        self._blocking_stop = False

    @abc.abstractmethod
    def _pre_stop_signal(self):
        pass  # pragma: no cover

    def on_stop_signal(self):
        """
        Non async method. Handle stop signals.
        """
        if self._pre_stop_signal():
            raise GracefulExit()

    @abc.abstractmethod
    async def _pre_loop(self):
        pass  # pragma: no cover

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

        try:
            await self._pre_loop()
        except Exception as e:
            self._running = False
            self._error = e
            raise
        start_fired = False
        while 1:
            if not start_fired and self.running and self.events.on_start:
                await self.events.on_start(time.time())
                start_fired = True
            if self._blocking_stop:
                break
            try:
                if self._do_stop:
                    await self._teardown()
                    self.events.on_stop and \
                        await self.events.on_stop(time.time())
                    break
                await self._run_loop()
                await asyncio.sleep(0.005)
                self._running = True
            except Exception as e:
                self._running = False
                self._error = e
                self.events.on_stop and \
                    await self.events.on_stop(time.time())
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
