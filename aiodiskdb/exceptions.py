class AioDiskDBException(Exception):
    pass


class NotRunningException(AioDiskDBException):
    pass


class ReadTimeoutException(AioDiskDBException):
    pass


class FailedToStopException(AioDiskDBException):
    pass


class DBNotInitializedException(AioDiskDBException):
    pass


class NotFoundException(AioDiskDBException):
    pass
