class AioDiskDBException(Exception):
    pass


class NotRunningException(AioDiskDBException):
    pass


class ReadTimeoutException(AioDiskDBException):
    pass
