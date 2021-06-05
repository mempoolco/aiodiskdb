class AioDiskDBException(Exception):
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
