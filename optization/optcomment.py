import hashlib
def saltedByHash(data, salt):
    """
    >>> salted_password('123', '123123123')
    5088545209380039756

    :param password:
    :param salt:
    :return:
    """
    # hash1 = hash(data+salt)
    hash1 = hashlib.md5(data+salt).hexdigest()
    return hash1

class Logger(object):
    def __init__(self, logQueue):
        self.logQueue = logQueue
    def debug(self, text):
        self.logQueue.put(('debug', text))
    def info(self, text):
        self.logQueue.put(('info', text))
    def warning(self, text):
        self.logQueue.put(('warning', text))
    def warn(self, text):
        self.logQueue.put(('warn', text))
    def error(self, text):
        self.logQueue.put(('error', text))
    def critical(self, text):
        self.logQueue.put(('critical', text))
