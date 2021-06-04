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
