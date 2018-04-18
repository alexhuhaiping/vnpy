# coding:utf-8

def saltedByHash(data, salt):
    """
    >>> salted_password('123', '123123123')
    5088545209380039756

    :param password:
    :param salt:
    :return:
    """
    hash1 = hash(data+salt)
    return hash1