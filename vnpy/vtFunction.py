# encoding: UTF-8

"""
包含一些开发中常用的函数
"""
import logging
import functools
import traceback
from vnpy import vtGlobal
import os
import decimal
import json
import time
from bson.codec_options import CodecOptions
import datetime as dt
from datetime import datetime
import pytz

LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')
CODEC_OPTIONS = CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE)
MAX_NUMBER = 10000000000000
MAX_DECIMAL = 4

#----------------------------------------------------------------------
def safeUnicode(value):
    """检查接口数据潜在的错误，保证转化为的字符串正确"""
    # 检查是数字接近0时会出现的浮点数上限
    if type(value) is int or type(value) is float:
        if value > MAX_NUMBER:
            value = 0
    
    # 检查防止小数点位过多
    if type(value) is float:
        d = decimal.Decimal(str(value))
        if abs(d.as_tuple().exponent) > MAX_DECIMAL:
            value = round(value, ndigits=MAX_DECIMAL)
    
    return str(value)

#----------------------------------------------------------------------
def loadMongoSetting():
    """载入MongoDB数据库的配置"""
    # fileName = 'VT_setting.json'
    # path = os.path.abspath(os.path.dirname(__file__))
    # fileName = os.path.join(path, fileName)
    setting = vtGlobal.VT_setting.copy()

    try:
        # f = file(fileName)
        # setting = json.load(f)
        host = setting['mongoHost']
        port = setting['mongoPort']
        logging = setting['mongoLogging']
    except Exception:
        host = 'localhost'
        port = 27017
        logging = False
        
    return host, port, logging

#----------------------------------------------------------------------
def todayDate():
    """获取当前本机电脑时间的日期"""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)    

 
def autoshutdown(clocks=None):
    """
    根据当前启动时间，设置自动关闭时间
    2:31 之前启动服务器，那么在2:31 会自动关闭；
    2:31 ~ 8:39 之间启动，会在8:39关闭，以此类推；
    20:39 之后关闭，会在次日 2:31 自动关闭


    >>> autoshutdown().closeTime == todayDate().replace(hour=15, minute=39)
    True

    :return:
    """

    clocks = clocks or [
        dt.time(2, 32),  # 夜盘后关闭
        dt.time(8, 39),  # 日盘前关闭
        dt.time(15, 17),  # 日盘后关闭
        dt.time(20, 39),  # 夜盘前关闭
    ]
    from threading import Thread

    now = datetime.now()
    today = todayDate()
    tomorrow = today + dt.timedelta(days=1)

    for t in clocks:
        if now.time() < t:
            # 关闭时间在当日
            closeTime = datetime.combine(today, t)
            break
    else:
        # 关闭时间在次日
        closeTime = datetime.combine(tomorrow, clocks[0])

    def shutdown(closeTime):
        # 子线程阻塞直到到达关闭时间
        while datetime.now() < closeTime:
            time.sleep(1)

    t = Thread(target=shutdown, args=(closeTime,))
    t.closeTime = closeTime
    t.start()
    return t


exceptionDic = {}
def exception(func):
    """
    用于捕获函数中的代码
    :param do:
     None       不抛出异常
     'raise'    继续抛出异常
    :return:
    """
    return func
    if func in exceptionDic:
        # 缓存
        return exceptionDic[func]

    @functools.wraps(func)
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            logger = logging.getLogger('root')
            logger.error(traceback.format_exc())
            time.sleep(0.3)
            raise

    # 缓存
    exceptionDic[func] = wrapper
    return wrapper


