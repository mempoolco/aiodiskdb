import abc
import asyncio
import time

import typing

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running, GracefulExit, logger
from aiodiskdb.local_types import EventsHandlers, ItemLocation


class AsyncLockable(metaclass=abc.ABCMeta):
    def __init__(self, *_, **__):
        super().__init__()
        self._write_lock = asyncio.Lock()
        self._transaction_lock = asyncio.Lock()
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
    def __init__(self, *_, timeout=0, **__):
        super().__init__(*_, **__)
        self._running = False
        self._error = False
        self._set_stop = False
        self._timeout = timeout
        self._blocking_stop = False
        self._stopped = False

    @abc.abstractmethod
    def _pre_stop_signal(self):
        pass  # pragma: no cover

    def on_stop_signal(self, *args):
        """
        Non async method. Handle stop signals.
        """
        logger.warning('Requested stop signal: %s', args)
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

    @property
    def stopped(self):
        return self._stopped

    @ensure_running(False)
    async def run(self):
        """
        Must be launched before using the Database as a non blocking task.
        example:
        loop.create_task(instance.run())
        loop.run_until_complete()
        """
        if self._stopped or self._set_stop or self._error:
            raise exceptions.InvalidDBStateException('DB instance not clean.')

        loop = asyncio.get_event_loop()
        try:
            await self._pre_loop()
        except Exception as e:
            self._running = False
            self._error = e
            self._stopped = True
            raise
        start_fired = False
        logger.info('Starting aiodiskdb')
        while 1:
            if not start_fired and self.running and self.events.on_start:
                loop.create_task(self.events.on_start(time.time()))
                start_fired = True
            if self._blocking_stop:
                break
            try:
                if self._set_stop:
                    await self._teardown()
                    self._stopped = True
                    break
                await self._run_loop()
                await asyncio.sleep(0.005)
                self._running = True
            except Exception as e:
                if self.events.on_failure:
                    loop.create_task(self.events.on_failure(time.time()))
                self._running = False
                self._error = e
                self.events.on_stop and \
                    loop.create_task(self.events.on_stop(time.time()))
                self._stopped = True
                raise
        self.events.on_stop and loop.create_task(self.events.on_stop(time.time()))
        logger.warning('Aiodiskdb is stopped')
        self._running = False
        self._stopped = True
        if self._error and isinstance(self._error, Exception):
            raise

    @ensure_running(True)
    async def stop(self):
        logger.debug('Requested stop')
        stop_requested_at = time.time()
        self._set_stop = True
        while time.time() - stop_requested_at < self._timeout:
            if not self._running:
                self._stopped = True
                return True
            await asyncio.sleep(0.1)
        raise exceptions.TimeoutException(f'Loop is still running after {self._timeout} seconds')


class AioDiskDBTransactionAbstract(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add(self, data: bytes) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def commit(self) -> typing.Iterable[ItemLocation]:
        pass  # pragma: no cover
