import asyncio
import os
import time

import typing
from collections import OrderedDict

from aiodiskdb import AioDiskDB, exceptions
from aiodiskdb.local_types import TempBufferData, TransactionStatus, Buffer, ItemLocation


class AioDiskDBTransaction:
    def __init__(self, session: AioDiskDB):
        self.session = session
        self._stack = list()
        self._status = TransactionStatus.INITIALIZED
        self._lock = asyncio.Lock()
        self._locations = list()

    async def _ensure_flush(self):
        pass

    def _bake_new_temp_buffer_data(self, res: typing.List[TempBufferData]) -> None:
        """
        The current buffer must be full if this method is called.
        Bake a new buffer contiguous to the current.
        """
        new_idx = res[-1].buffer.index + 1
        res.append(
            TempBufferData(
                buffer=Buffer(
                    index=new_idx,
                    size=0,
                    items=0,
                    file_size=self.session.GENESIS_BYTES_LENGTH,
                    data=self.session._genesis_bytes,
                ),
                idx={new_idx: dict()}
            )
        )

    def _bake_temp_buffer_data(self) -> typing.List[TempBufferData]:
        """
        Bake buffer data so that session._save_data_to_disk function could handle it with no changes.
        """
        assert self.session._buffers[-1].size == 0
        current_buffer = self.session._buffers[-1]
        res = [
            TempBufferData(
                buffer=current_buffer,
                idx={current_buffer.index: self.session._buffer_index}
            )
        ]
        max_file_size = self.session._max_file_size
        while self._stack:
            data_size = len(self._stack[0])
            buffer: Buffer = res[-1].buffer
            if data_size + buffer.file_size > max_file_size:
                self._bake_new_temp_buffer_data(res)
                continue
            data = self._stack.pop(0)
            self._locations.append(
                ItemLocation(buffer.index, buffer.file_size, data_size)
            )
            buffer.data += data
            buffer.size += data_size
            buffer.file_size += data_size
        return res

    async def _write_db_snapshot(self, timestamp: int):
        """
        Persist the current status of files before appending data.
        It will be reverted in case of a commit failure.
        """
        sizes = map(
            lambda _filename: [_filename, os.path.getsize(os.path.join(self.session.path, _filename))],
            os.listdir()
        )
        with open(os.path.join(self.session.path, f'.transaction-snapshot-{timestamp}'), 'wb') as f:
            for s in sizes:
                filename, size = s
                data = filename.encode() + b';' + size.to_bytes(4, 'little') + '\n'
                f.write(data)

    async def _clean_db_snapshot(self, timestamp: int):
        """
        The Transaction was successfully and there is no reason to keep the snapshot.
        """
        os.remove(os.path.join(self.session.path, f'.transaction-snapshot-{timestamp}'))

    async def _update_session_buffer(self, temp_buffer_data: TempBufferData):
        """
        This is fired after a Transaction is successfully saved to disk.
        Set the session buffer to the latest baked by the Transaction.
        """
        self.session._buffers[-1] = temp_buffer_data.buffer
        self.session._buffer_index = OrderedDict(temp_buffer_data.buffer)

    async def add(self, data: bytes):
        """
        Add some data to a transaction.
        Data added into this scope is not available into the session
        until the transaction is committed.
        """
        await self._lock.acquire()
        try:
            if len(data) > self.session._max_file_size:
                raise exceptions.WriteFailedException(
                    f'File too big: {len(data)} > {self.session._max_file_size}'
                )
            if self._status == TransactionStatus.DONE:
                raise exceptions.TransactionAlreadyCommittedException
            elif self._status == TransactionStatus.ONGOING:
                raise exceptions.TransactionCommitOnGoingException

            self._stack.append(data)
        finally:
            self._lock.release()

    async def commit(self) -> typing.Iterable[ItemLocation]:
        """
        Commit a transaction, save to data the _stack content, using TempBufferData objects.
        """
        if self._status == TransactionStatus.DONE:
            raise exceptions.TransactionAlreadyCommittedException
        elif self._status == TransactionStatus.ONGOING:
            raise exceptions.TransactionCommitOnGoingException
        elif not self._stack:
            raise exceptions.EmptyTransactionException
        await self.session._transaction_lock.acquire()
        await self._lock.acquire()
        now = int(time.time())
        try:
            await self._do_commit(now)
            locations = self._locations
            self._locations = []
            return locations
        finally:
            self.session._transaction_lock.release()
            self._lock.release()

    async def _do_commit(self, timestamp: int):
        """
        Part of the <commit> method.
        Actually saves data to disk.
        """
        self._status = TransactionStatus.ONGOING
        await self._ensure_flush()
        assert not self.session._buffers[-1].size
        await self._write_db_snapshot(timestamp)
        temp_buffers_data = self._bake_temp_buffer_data()
        assert temp_buffers_data
        temp_buffer_data = None
        for temp_buffer_data in temp_buffers_data:
            asyncio.get_event_loop().run_in_executor(
                None,
                self.session._save_buffer_to_disk,
                temp_buffer_data
            )
        assert temp_buffer_data
        self._update_session_buffer(temp_buffer_data)
        await self._clean_db_snapshot(timestamp)
        self._status = TransactionStatus.DONE
