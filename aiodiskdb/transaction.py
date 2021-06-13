import asyncio
import time

import typing
from collections import OrderedDict, deque

from aiodiskdb import AioDiskDB, exceptions
from aiodiskdb.abstracts import AioDiskDBTransactionAbstract
from aiodiskdb.internals import ensure_async_lock, logger
from aiodiskdb.local_types import TempBufferData, TransactionStatus, Buffer, ItemLocation, LockType


class AioDiskDBTransaction(AioDiskDBTransactionAbstract):
    def __init__(self, session: AioDiskDB):
        self.session = session
        self._stack = deque()
        self._status = TransactionStatus.INITIALIZED
        self._lock = asyncio.Lock()
        self._locations = list()
        logger.debug('Initialized a new transaction')

    @property
    def _transaction_lock(self):
        return self.session._transaction_lock

    def _bake_new_temp_buffer_data(self, res: typing.List[TempBufferData]) -> None:
        """
        The current buffer must be full if this method is called.
        Bake a new buffer contiguous to the current.
        """
        new_idx = res[-1].buffer.index + 1
        res.append(
            TempBufferData(
                self.session._bake_new_buffer(new_idx),
                idx={new_idx: dict()}
            )
        )

    def _bake_temp_buffer_data(self) -> typing.List[TempBufferData]:
        """
        Bake buffer data so that <session._save_data_to_disk>
        function could handle it with no changes.
        There is no buffer size limit while in a Transaction,
        only the file size limit is respected.
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
        while len(self._stack):
            data = self._stack.popleft()
            data_size = len(data)
            buffer: Buffer = res[-1].buffer
            if data_size + buffer.file_size > max_file_size:
                self._bake_new_temp_buffer_data(res)
                self._stack.insert(0, data)
                continue
            self._locations.append(
                ItemLocation(buffer.index, buffer.file_size, data_size)
            )
            buffer.data += data
            buffer.size += data_size
            buffer.file_size += data_size
        return res

    async def _update_session_buffer(self, temp_buffer_data: TempBufferData):
        """
        This is fired after a Transaction is successfully saved to disk.
        Set the session buffer to the latest baked by the Transaction.
        """
        temp_buffer_data.buffer.data = b''
        temp_buffer_data.buffer.size = 0
        temp_buffer_data.buffer.head = not temp_buffer_data.buffer.file_size
        self.session._buffers[-1] = temp_buffer_data.buffer
        self.session._buffer_index = OrderedDict({temp_buffer_data.buffer.index: dict()})

    def add(self, data: bytes):
        """
        Add some data to a transaction.
        Data added into this scope is not available into the session
        until the transaction is committed.
        """
        logger.debug('Adding item to transaction: %s', len(data))
        if len(data) > self.session._max_file_size:
            raise exceptions.WriteFailedException(
                f'File too big: {len(data)} > {self.session._max_file_size}'
            )
        if self._status == TransactionStatus.DONE:
            raise exceptions.TransactionAlreadyCommittedException
        elif self._status == TransactionStatus.ONGOING:
            raise exceptions.TransactionCommitOnGoingException

        self._stack.append(data)

    async def commit(self) -> typing.Iterable[ItemLocation]:
        """
        Commit a transaction, save to data the <_stack> content, using TempBufferData objects.
        """
        logger.debug('Requested transaction commit, tx size: %s', len(self._stack))
        await self._lock.acquire()
        try:
            if self._status == TransactionStatus.DONE:
                raise exceptions.TransactionAlreadyCommittedException
            elif not self._stack:
                raise exceptions.EmptyTransactionException
            now = int(time.time()*1000)

            await self.session._flush_buffer()
            await self._do_commit(now)
            locations = self._locations
            self._locations = list()
            logger.debug('Transaction commit done')
            return locations
        finally:
            self._lock.release()

    @ensure_async_lock(LockType.TRANSACTION)
    async def _do_commit(self, timestamp: int):
        """
        Part of the <commit> method.
        Actually saves data to disk.
        """
        self._status = TransactionStatus.ONGOING
        assert not self.session._buffers[-1].size
        idx_involved_in_batch = list()
        temp_buffers_data = self._bake_temp_buffer_data()
        for buff in temp_buffers_data:
            idx_involved_in_batch.extend(list(buff.idx))
        await self.session._write_db_checkpoint(
            timestamp, *set(idx_involved_in_batch)
        )
        assert temp_buffers_data
        temp_buffer_data = None
        for temp_buffer_data in temp_buffers_data:
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.session._save_buffer_to_disk,
                temp_buffer_data
            )
        assert temp_buffer_data
        await self._update_session_buffer(temp_buffer_data)
        await self.session._clean_db_checkpoint(timestamp)
        self._status = TransactionStatus.DONE
