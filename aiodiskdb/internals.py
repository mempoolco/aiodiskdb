import abc
import asyncio
import functools

from aiodiskdb import exceptions
from aiodiskdb.types import LockType


def ensure_running(expected_state: bool):
    def _decorator(f):
        @functools.wraps(f)
        def _checker(self, *a, **kw):
            if self.running != expected_state:
                raise exceptions.NotRunningException
            return f(*a, **kw)
        return _checker
    return _decorator


def ensure_future(f):
    async def _ensure(*a, **kw):
        done, pending = await f(*a, **kw)
        if not done:
            pending.cancel()
            raise exceptions.ReadTimeoutException
        return done.result()
    return _ensure


def ensure_async_lock(lock_type: LockType):
    def _decorator(f):
        async def _ensure(self, *a, **kw):
            if lock_type == LockType.READ:
                await self._read_lock.acquire()
                self._incr_read()
                if self._reads_count == 1:
                    await self.write_lock.acquire()
                self.read_lock.release()
                try:
                    return f(*a, **kw)
                finally:
                    await self.read_lock.acquire()
                    self._decr_read()
                    if not self._reads_count:
                        self.write_lock.release()
                    self.read_lock.release()
            elif lock_type == LockType.WRITE:
                await self.write_lock.acquire()
                try:
                    return f(*a, **kw)
                finally:
                    self.write_lock.release()
            else:
                raise ValueError('Value must be LockType.READ or WRITE')
        return _ensure
    return _decorator


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
    def __init__(self):
        self._running = False
        self._error = False

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
                await self._run_loop()
                await asyncio.sleep(0.005)
            except Exception as e:
                self._running = False
                self._error = e
                raise

    @abc.abstractmethod
    async def _run_loop(self):
        pass  # pragma: no cover
