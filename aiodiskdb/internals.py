from aiodiskdb import exceptions
from aiodiskdb.local_types import LockType


def ensure_running(expected_state: bool):
    def _decorator(f):
        async def _ensure(self, *a, **kw):
            if self.running != expected_state:
                raise exceptions.NotRunningException
            return await f(self, *a, **kw)
        return _ensure
    return _decorator


def ensure_async_lock(lock_type: LockType):
    def _decorator(f):
        async def _ensure(self, *a, **kw):
            if lock_type == LockType.READ:
                await self._read_lock.acquire()
                self._incr_read()
                if self._reads_count == 1:
                    await self._write_lock.acquire()
                self._read_lock.release()
                try:
                    return await f(self, *a, **kw)
                finally:
                    await self._read_lock.acquire()
                    self._decr_read()
                    if not self._reads_count:
                        self._write_lock.release()
                    self._read_lock.release()
            elif lock_type == LockType.WRITE:
                await self._write_lock.acquire()
                try:
                    return await f(self, *a, **kw)
                finally:
                    self._write_lock.release()
            else:
                raise ValueError('Value must be LockType.READ or WRITE')
        return _ensure
    return _decorator


class GracefulExit(SystemExit):
    code = 1
