# encoding: UTF-8

"""
包含一些开发中常用的函数
"""
import traceback
import logging
import functools
import os
import decimal
import arrow
import time
import datetime
import pytz
import tradingtime as tt

LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')
MAX_NUMBER = 10000000000000
MAX_DECIMAL = 4


# ----------------------------------------------------------------------
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

    return unicode(value)


# ----------------------------------------------------------------------
def todayDate():
    """获取当前本机电脑时间的日期"""
    # return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return arrow.now().datetime.replace(hour=0, minute=0, second=0, microsecond=0)


# 图标路径
iconPathDict = {}

path = os.path.abspath(os.path.dirname(__file__))
for root, subdirs, files in os.walk(path):
    for fileName in files:
        if '.ico' in fileName:
            iconPathDict[fileName] = os.path.join(root, fileName)


# ----------------------------------------------------------------------
def loadIconPath(iconName):
    """加载程序图标路径"""
    global iconPathDict
    return iconPathDict.get(iconName, '')


# ----------------------------------------------------------------------
def getTempPath(name):
    """获取存放临时文件的路径"""
    tempPath = os.path.join(os.getcwd(), 'temp')
    if not os.path.exists(tempPath):
        os.makedirs(tempPath)

    path = os.path.join(tempPath, name)
    return path


# JSON配置文件路径
jsonPathDict = {}


# ----------------------------------------------------------------------
def getJsonPath(name, moduleFile):
    """
    获取JSON配置文件的路径：
    1. 优先从当前工作目录查找JSON文件
    2. 若无法找到则前往模块所在目录查找
    """
    currentFolder = os.getcwd()
    currentJsonPath = os.path.join(currentFolder, name)
    if os.path.isfile(currentJsonPath):
        jsonPathDict[name] = currentJsonPath
        return currentJsonPath

    moduleFolder = os.path.abspath(os.path.dirname(moduleFile))
    moduleJsonPath = os.path.join(moduleFolder, '.', name)
    jsonPathDict[name] = moduleJsonPath
    return moduleJsonPath


exceptionDic = {}
def exception(func):
    """
    用于捕获函数中的代码
    :param do:
     None       不抛出异常
     'raise'    继续抛出异常
    :return:
    """
    if func in exceptionDic:
        # 缓存
        return exceptionDic[func]

    @functools.wraps(func)
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            logger = logging.getLogger()
            logger.error(traceback.format_exc())
            raise

    # 缓存
    exceptionDic[func] = wrapper
    return wrapper



def waitToContinue(vtSymbol, now):
    """
    用夹逼法获得最近的一个连续竞价时间段
    :return:
    """
    if tt.get_trading_status(vtSymbol, now) == tt.continuous_auction:
        # 已经处于连续交易时间中
        return now

    c = now
    while tt.get_trading_status(vtSymbol, c) != tt.continuous_auction:
        # 1分钟步长往前探索
        c += datetime.timedelta(minutes=1)

    while tt.get_trading_status(vtSymbol, c) == tt.continuous_auction:
        # 1秒钟步长往回探索，得到连续交易前1秒
        c -= datetime.timedelta(seconds=1)

    # 增加1秒
    c += datetime.timedelta(seconds=1)
    return c
