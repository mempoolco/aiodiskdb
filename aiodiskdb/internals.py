import logging

from aiodiskdb import exceptions
from aiodiskdb.local_types import LockType


def ensure_running(expected_state: bool, allow_stop_state=False):
    def _decorator(f):
        async def _ensure(self, *a, **kw):
            if self._error:
                raise exceptions.NotRunningException('Database is in ERROR state')
            if expected_state and self._do_stop and not allow_stop_state:
                raise exceptions.NotRunningException('Database is in STOP state')
            if self.running != expected_state:
                raise exceptions.NotRunningException('Database it not running')
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
                in_transaction = self._transaction_lock.locked()
                if not in_transaction:
                    await self._transaction_lock.acquire()
                await self._write_lock.acquire()
                try:
                    return await f(self, *a, **kw)
                finally:
                    if not in_transaction:
                        self._transaction_lock.release()
                    self._write_lock.release()
            elif lock_type == LockType.TRANSACTION:
                await self._transaction_lock.acquire()
                try:
                    return await f(self, *a, **kw)
                finally:
                    self._transaction_lock.release()
            else:
                raise ValueError('Value must be LockType.READ or WRITE')
        return _ensure
    return _decorator


class GracefulExit(SystemExit):
    code = 1


logger = logging.getLogger('aiodiskdb')
