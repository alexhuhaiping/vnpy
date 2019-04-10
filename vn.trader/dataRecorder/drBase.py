# encoding: UTF-8

'''
本文件中包含的数据格式和CTA模块通用，用户有必要可以自行添加格式。
'''


import pymongo

# 把vn.trader根目录添加到python环境变量中
import sys
import datetime
import pytz
import tradingtime as tt
import arrow

sys.path.append('..')

# 数据库名称
# SETTING_DB_NAME = 'VnTrader_Setting_Db'
# TICK_DB_NAME = 'VnTrader_Tick_Db'
# DAILY_DB_NAME = 'VnTrader_Daily_Db'
# MINUTE_DB_NAME = 'VnTrader_1Min_Db'
SETTING_DB_NAME = 'ctp'
TICK_DB_NAME = 'ctp'
DAILY_DB_NAME = 'ctp'
MINUTE_DB_NAME = 'ctp'
CONTRACT_DB_NAME = 'ctp'
# TICK_COLLECTION_SUBFIX = 'tick'
BAR_COLLECTION_SUBFIX = 'min'
BAR_COLLECTION_NAME = 'bar_1min'
BAR_COLLECTION_NAME_BAK = 'bar_1min_bak'
CONTRACT_INFO_COLLECTION_NAME = 'contract'

# CTA引擎中涉及的数据类定义
from vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT

LOCAL_TZINFO = pytz.timezone('Asia/Shanghai')


########################################################################
class DrBarData(object):
    """K线数据"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING  # vt系统代码
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所
        self.tradingDay = EMPTY_STRING

        self.open = EMPTY_FLOAT  # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT

        self.upperLimit = EMPTY_FLOAT  # 涨停价
        self.lowerLimit = EMPTY_FLOAT  # 跌停价

        self.date = EMPTY_STRING  # bar开始的时间，日期
        self.time = EMPTY_STRING  # 时间
        self.datetime = None  # python的datetime时间对象
        self.last = None  # 最后一个tick 的 datetime

        self.volume = EMPTY_INT  # 成交量
        self.openInterest = EMPTY_INT  # 持仓量

    def tickNew(self, drTick):
        """
        一个新的bar

        :param drTick:
        :return:
        """
        bar = self
        bar.vtSymbol = drTick.vtSymbol
        bar.symbol = drTick.symbol
        bar.exchange = drTick.exchange

        bar.open = drTick.lastPrice
        bar.high = drTick.lastPrice
        bar.low = drTick.lastPrice
        bar.close = drTick.lastPrice

        bar.last = drTick.datetime
        bar.vtSymbol = drTick.vtSymbol
        isTrading, tradingDay = tt.get_tradingday(drTick.datetime)

        if not tradingDay.tzinfo:
            tradingDay = LOCAL_TZINFO.localize(tradingDay)
        bar.tradingDay = tradingDay

        if not isTrading:
            # if __debug__:
            #     print(u'{} # 非交易时间 bar 不保存'.format(self.symbol))

            bar.vtSymbol = None

        bar.datetime = self.dt2DTM(drTick.datetime)
        assert isinstance(bar.datetime, datetime.datetime)

        if tt.get_trading_status(self.symbol, bar.datetime) != tt.continuous_auction:
            bar.vtSymbol = None

        bar.date = bar.datetime.strftime('%Y%m%d')
        bar.time = bar.datetime.strftime('%H:%M:%S')
        bar.openInterest = drTick.openInterest
        if bar.volume == drTick.volume:
            # if __debug__:
            #     print(u'{} bar 没更新，不保存'.format(self.symbol))
            # bar 没更新，不保存
            bar.vtSymbol = None
        bar.volume = drTick.volume

        now = arrow.now()
        timeDelta = drTick.datetime - now
        deltaSec = abs(timeDelta.total_seconds())

        if deltaSec > 60 * 5:
            # 跟当前时间差超过5分钟，则认为是无效的 tick
            bar.vtSymbol = None

        bar.upperLimit = drTick.upperLimit
        bar.lowerLimit = drTick.lowerLimit

    def tickUpdate(self, drTick):
        """
        根据 tick 更新 bar
        :param drTick:
        :return:
        """
        bar = self
        bar.last = drTick.datetime
        bar.high = max(bar.high, drTick.lastPrice)
        bar.low = min(bar.low, drTick.lastPrice)
        bar.close = drTick.lastPrice
        bar.upperLimit = drTick.upperLimit
        bar.lowerLimit = drTick.lowerLimit
        if bar.vtSymbol is None and bar.volume != drTick.volume:
            # if __debug__:
            #     print(u'{} bar 更新，要保存'.format(self.symbol))
            bar.vtSymbol = drTick.vtSymbol
        bar.volume = drTick.volume
        bar.openInterest = drTick.openInterest

    @staticmethod
    def dt2DTM(dt):
        """
        将某一时刻的 dt 转为对应的 dtm
        :return:
        """
        assert isinstance(dt, datetime.datetime)
        if dt.second == 0 and dt.microsecond == 0:
            # 11:29:00.500 ~ 11:30:00 算作 bar 11:30:00.
            # 第一个bar 是 9:01:00，最后一个 bar 是 11:30:00
            return dt
        else:
            return dt.replace(second=0, microsecond=0) + datetime.timedelta(seconds=60)

    def toSave(self):
        """

        :return:
        """
        return {
            'symbol': self.symbol,
            'tradingDay': self.tradingDay,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'upperLimit': self.upperLimit,
            'lowerLimit': self.lowerLimit,
            'date': self.date,
            'time': self.time,
            'datetime': self.datetime,
            'volume': self.volume,
            'openInterest': self.openInterest,
        }


########################################################################
class DrTickData(object):
    """Tick数据"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING  # vt系统代码
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码

        # 成交数据
        self.lastPrice = EMPTY_FLOAT  # 最新成交价
        self.volume = EMPTY_INT  # 最新成交量
        self.openInterest = EMPTY_INT  # 持仓量

        self.upperLimit = EMPTY_FLOAT  # 涨停价
        self.lowerLimit = EMPTY_FLOAT  # 跌停价

        # tick的时间
        self.date = EMPTY_STRING  # 日期
        self.time = EMPTY_STRING  # 时间
        self.datetime = None  # python的datetime时间对象

        # 五档行情
        self.bidPrice1 = EMPTY_FLOAT
        self.bidPrice2 = EMPTY_FLOAT
        self.bidPrice3 = EMPTY_FLOAT
        self.bidPrice4 = EMPTY_FLOAT
        self.bidPrice5 = EMPTY_FLOAT

        self.askPrice1 = EMPTY_FLOAT
        self.askPrice2 = EMPTY_FLOAT
        self.askPrice3 = EMPTY_FLOAT
        self.askPrice4 = EMPTY_FLOAT
        self.askPrice5 = EMPTY_FLOAT

        self.bidVolume1 = EMPTY_INT
        self.bidVolume2 = EMPTY_INT
        self.bidVolume3 = EMPTY_INT
        self.bidVolume4 = EMPTY_INT
        self.bidVolume5 = EMPTY_INT

        self.askVolume1 = EMPTY_INT
        self.askVolume2 = EMPTY_INT
        self.askVolume3 = EMPTY_INT
        self.askVolume4 = EMPTY_INT
        self.askVolume5 = EMPTY_INT
