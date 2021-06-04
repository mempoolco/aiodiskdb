import abc
import asyncio
import functools

from aiodiskdb import exceptions


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


def ensure_async_lock(lock_type: str):
    def _decorator(f):
        async def _ensure(self, *a, **kw):
            if lock_type == 'read':
                await self.locks['read'].acquire()
                self.locks['reads_count'] += 1
                if self.locks['reads_count'] == 1:
                    await self.locks['write'].acquire()
                self.locks['read'].release()
                try:
                    return f(*a, **kw)
                finally:
                    await self.locks['read'].acquire()
                    self.locks['reads_count'] -= 1
                    if not self.locks['reads_count']:
                        self.lock['write'].release()
                    self.lock['read'].release()
            elif lock_type == 'write':
                await self.locks['write'].acquire()
                try:
                    return f(*a, **kw)
                finally:
                    self.locks['write'].release()
            else:
                raise ValueError('Value must be read\write')
        return _ensure
    return _decorator


class AsyncLockable:
    def __init__(self):
        self._locks = dict(write=asyncio.Lock(), read=asyncio.Lock, reads_count=[])


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
