# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''
from __future__ import division

import time
import logging
import logging.config
from bson.codec_options import CodecOptions
from datetime import datetime, timedelta
import pytz

import pymongo
import pandas as pd

from vnpy.trader.vtConstant import *
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtObject import VtTickData, VtBarData
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine as VTBacktestingEngine
from vnpy.trader.vtFunction import getTempPath, getJsonPath
from vnpy.trader.vtGateway import VtOrderData, VtTradeData
from .ctaBase import *

# 读取日志配置文件
loggingConFile = 'logging.conf'
loggingConFile = getJsonPath(loggingConFile, __file__)
logging.config.fileConfig(loggingConFile)


########################################################################
class BacktestingEngine(VTBacktestingEngine):
    """
    重写的回测引擎类
    CTA回测引擎
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    """

    TICK_MODE = 'tick'
    BAR_MODE = 'bar'

    LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # 本地停止单
        self.log = logging.getLogger('ctabacktesting')
        super(BacktestingEngine, self).__init__()

        self._datas = []  # 1min bar 的原始数据
        self.datas = []  # 聚合后，用于回测的数据

        self._initData = []  # 初始化用的数据, 最早的1min bar
        self.initData = []  # 聚合后的数据，真正用于跑回测的数据

        self.dbClient = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'],
                                            connectTimeoutMS=500)

        ctpdb = self.dbClient[globalSetting['mongoCtpDbn']]
        ctpdb.authenticate(globalSetting['mongoUsername'], globalSetting['mongoPassword'])

        # 1min bar collection
        self.ctpCol1minBar = ctpdb['bar_1min'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.LOCAL_TIMEZONE))

        # 日线的 collection
        self.ctpCol1dayBar = ctpdb['bar_1day'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.LOCAL_TIMEZONE))

        self.loadHised = False  # 是否已经加载过了历史数据
        self.barPeriod = '1T'  # 默认是1分钟 , 15T 是15分钟， 1H 是1小时，1D 是日线

        logging.Formatter.converter = self.barTimestamp
    #------------------------------------------------
    # 通用功能
    #------------------------------------------------

    #----------------------------------------------------------------------
    def roundToPriceTick(self, price):
        """取整价格到合约最小价格变动"""
        if not self.priceTick:
            return price

        newPrice = round(price/self.priceTick, 0) * self.priceTick
        return newPrice

    #----------------------------------------------------------------------
    def output(self, content):
        """输出内容"""
        self.log.warning(content)

    #------------------------------------------------

    def resample(self):
        """
        聚合数据
        :return:
        """

        self.initData = self._resample(self.barPeriod, self._initData)
        self.datas = self._resample(self.barPeriod, self._datas)

    @classmethod
    def _resample(cls, barPeriod, datas):
        """

        :param datas:
        :return:
        """
        # 日线级别的聚合，使用 tradingDay 作为索引
        if barPeriod == '1T':
            # 不需要聚合
            return datas

        # 使用 pandas 来聚合
        df = pd.DataFrame([d.dump() for d in datas])

        if barPeriod.endswith('D'):
            # 日线的聚合，使用 tradingDay 作为索引进行
            rdf = df.set_index('tradingDay')
            rdf = cls._resampleSeries(rdf, barPeriod)
            rdf = rdf.dropna(inplace=False)

            # 添加 tradingDay
            # rdf.tradingDay = rdf.index

            rdf.index.name = 'datetime'
            rdf = rdf.reset_index(drop=False, inplace=False)
        else:
            # 日线以下的级别，使用 datetime 来聚合
            rdf = df.set_index('datetime')
            rdf = cls._resampleSeries(rdf, barPeriod)
            rdf = rdf.dropna(inplace=False)

            # 添加 tradingDay
            # td = df.tradingDay.resample(barPeriod, closed='right', label='right').last().dropna()
            # rdf.tradingDay = td.apply(lambda td: td.tz_localize('UTC').tz_convert('Asia/Shanghai'))

            rdf = rdf.reset_index(drop=False, inplace=False)

        # 补充 date 字段和 time 字段
        rdf['date'] = rdf.datetime.apply(lambda dt: dt.strftime('%Y%m%d'))
        rdf['time'] = rdf.datetime.apply(lambda dt: dt.strftime('%H:%M:%S'))

        # 重新生成 bar
        datas = []
        for d in rdf.to_dict('records'):
            data = VtBarData()
            data.load(d)
            datas.append(data)
        return datas

    @classmethod
    def _resampleSeries(cls, rdf, barPeriod):
        r = rdf.resample(barPeriod, closed='right', label='right')
        o = r.open.first()
        h = r.high.max()
        l = r.low.min()
        c = r.close.last()
        v = r.volume.sum()
        oi = r.openInterest.last()

        return pd.DataFrame(
            {
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': v,
                'openInterest': oi,
            },
        )

    # 参数设置相关
    #------------------------------------------------

    #----------------------------------------------------------------------
    def setStartDate(self, startDate='20100416', initDays=10):
        """设置回测的启动日期"""
        self.startDate = startDate
        self.initDays = initDays

        self.dataStartDate = self.LOCAL_TIMEZONE.localize(datetime.strptime(startDate, '%Y%m%d'))

        # initTimeDelta = timedelta(initDays)
        # 要获取 initDays 个交易日的数据
        sql = {
            'symbol': self.symbol,
            'tradingDay': {
                '$gte': self.dataStartDate,
            }
        }
        cursor = self.ctpCol1dayBar.find(sql, {'_id': 0})
        # 顺序排列
        cursor.sort('tradingDay')

        cursor.skip(initDays)
        dayBar = cursor.next()

        self.strategyStartDate = dayBar['tradingDay']

        self.log.warning(u'strategyStartDate {}'.format(str(self.strategyStartDate)))

    def setEndDate(self, endDate=''):
        """设置回测的结束日期"""
        self.endDate = endDate

        if endDate:
            self.dataEndDate = self.LOCAL_TIMEZONE.localize(datetime.strptime(endDate, '%Y%m%d'))

    def setBarPeriod(self, barPeriod):
        """

        :return:
        """
        periodTypes = ['T', 'H', 'D']
        if barPeriod[-1] not in periodTypes:
            raise ValueError(u'周期应该为 {} , 如 15T 是15分钟K线这种格式'.format(str(periodTypes)))

        self.barPeriod = barPeriod

    # ------------------------------------------------
    # 数据回放相关
    #------------------------------------------------

    #----------------------------------------------------------------------
    def loadHistoryData(self):
        """载入历史数据"""
        self.loadHised = True
        collection = self.ctpCol1minBar

        self.log.info(u'开始载入数据')

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = VtBarData
            func = self.newBar
        else:
            dataClass = VtTickData
            func = self.newTick

        # 载入初始化需要用的数据
        flt = {'tradingDay': {'$gte': self.dataStartDate,
                              '$lt': self.strategyStartDate},
               'symbol': self.symbol}

        initCursor = collection.find(flt, {'_id': 0})
        initCount = initCursor.count()
        self.log.info(u'预加载数据量 {}'.format(initCount))

        # 将数据从查询指针中读取出，并生成列表
        self._initData = []  # 清空initData列表
        for d in initCursor:
            data = dataClass()
            data.load(d)
            self._initData.append(data)

        self._initData.sort(key=lambda data: data.datetime)

        # 载入回测数据
        if not self.dataEndDate:
            flt = {'tradingDay': {'$gte': self.strategyStartDate}, 'symbol': self.symbol}  # 数据过滤条件
        else:
            flt = {'tradingDay': {'$gte': self.strategyStartDate,
                                  '$lte': self.dataEndDate},
                   'symbol': self.symbol}

        self.dbCursor = collection.find(flt, {'_id': 0})

        # count = self.dbCursor.count()

        _datas = []
        for d in self.dbCursor:
            data = dataClass()
            data.load(d)
            _datas.append(data)

        # 根据日期排序
        _datas.sort(key=lambda data: data.datetime)
        self._datas = _datas
        self.log.info(u'载入完成，数据量：%s' % (len(_datas)))

    # ----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        # 载入历史数据
        if not self.loadHised:
            self.loadHistoryData()

        # 聚合数据
        self.resample()

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = VtBarData
            func = self.newBar
        else:
            dataClass = VtTickData
            func = self.newTick

        self.log.info(u'开始回测')

        self.strategy.inited = True
        self.strategy.onInit()
        self.log.info(u'策略初始化完成')

        self.strategy.trading = True
        self.strategy.onStart()
        self.log.info(u'策略启动完成')

        self.log.info(u'开始回放数据')

        # for d in self.dbCursor:
        for data in self.datas:
            try:
                func(data)
            except:
                self.log.error(u'异常 bar: {}'.format(data.datetime))
                raise

        self.log.info(u'数据回放结束')
        self.strategy.trading = False

    def loadBar(self, symbol, collectionName, barNum, barPeriod=1):
        """直接返回初始化数据列表中的Bar"""
        return self.initData

    def barTimestamp(self, *args, **kwargs):
        if self.dt:
            return self.dt.timetuple()
        else:
            return time.localtime(time.time())

    # ----------------------------------------------------------------------
    def crossStopOrder(self):
        """基于最新数据撮合停止单"""
        # 先确定会撮合成交的价格，这里和限价单规则相反
        if self.mode == self.BAR_MODE:
            buyCrossPrice = self.bar.high  # 若买入方向停止单价格低于该价格，则会成交
            sellCrossPrice = self.bar.low  # 若卖出方向限价单价格高于该价格，则会成交
            bestCrossPrice = self.bar.open  # 最优成交价，买入停止单不能低于，卖出停止单不能高于
        else:
            buyCrossPrice = self.tick.lastPrice
            sellCrossPrice = self.tick.lastPrice
            bestCrossPrice = self.tick.lastPrice

        # 遍历停止单字典中的所有停止单
        for stopOrderID, so in self.workingStopOrderDict.items():
            # 判断是否会成交
            buyCross = so.direction == DIRECTION_LONG and so.price <= buyCrossPrice
            sellCross = so.direction == DIRECTION_SHORT and so.price >= sellCrossPrice

            # 如果发生了成交
            if buyCross or sellCross:
                # 更新停止单状态，并从字典中删除该停止单
                so.status = STOPORDER_TRIGGERED
                if stopOrderID in self.workingStopOrderDict:
                    del self.workingStopOrderDict[stopOrderID]

                    # 推送成交数据
                self.tradeCount += 1  # 成交编号自增1
                tradeID = str(self.tradeCount)
                trade = VtTradeData()
                trade.vtSymbol = so.vtSymbol
                trade.tradeID = tradeID
                trade.vtTradeID = tradeID

                if buyCross:
                    self.strategy.pos += so.volume
                    trade.price = max(bestCrossPrice, so.price)
                else:
                    self.strategy.pos -= so.volume
                    trade.price = min(bestCrossPrice, so.price)

                self.limitOrderCount += 1
                orderID = str(self.limitOrderCount)
                trade.orderID = orderID
                trade.vtOrderID = orderID
                trade.direction = so.direction
                trade.offset = so.offset
                trade.volume = so.volume
                trade.tradeTime = self.dt.strftime('%H:%M:%S')
                trade.dt = self.dt

                self.tradeDict[tradeID] = trade

                # 推送委托数据
                order = VtOrderData()
                order.vtSymbol = so.vtSymbol
                order.symbol = so.vtSymbol
                order.orderID = orderID
                order.vtOrderID = orderID
                order.direction = so.direction
                order.offset = so.offset
                order.price = so.price
                order.totalVolume = so.volume
                order.tradedVolume = so.volume
                order.status = STATUS_ALLTRADED
                order.orderTime = trade.tradeTime

                self.limitOrderDict[orderID] = order

                so.vtOrderID = orderID

                # 按照顺序推送数据
                self.strategy.onStopOrder(so)
                self.strategy.onOrder(order)
                self.strategy.onTrade(trade)
