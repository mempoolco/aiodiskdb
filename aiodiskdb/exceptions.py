class AioDiskDBException(Exception):
    pass


class RunningException(AioDiskDBException):
    pass


class NotRunningException(AioDiskDBException):
    pass


class WriteTimeoutException(AioDiskDBException):
    pass


class ReadTimeoutException(AioDiskDBException):
    pass


class FailedToStopException(AioDiskDBException):
    pass


class DBNotInitializedException(AioDiskDBException):
    pass


class InvalidDataFileException(AioDiskDBException):
    pass


class ReadOnlyDatabaseException(AioDiskDBException):
    pass


class FilesInconsistencyException(AioDiskDBException):
    pass


class WriteFailedException(AioDiskDBException):
    pass


class InvalidConfigurationException(AioDiskDBException):
    pass


class EmptyTransactionException(AioDiskDBException):
    pass


class TransactionCommitOnGoingException(AioDiskDBException):
    pass


class TransactionAlreadyCommittedException(AioDiskDBException):
    pass


class PendingSnapshotException(AioDiskDBException):
    pass


class InvalidDBStateException(AioDiskDBException):
    pass


class IndexDoesNotExist(AioDiskDBException):
    pass
