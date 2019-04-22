# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''



from collections import OrderedDict
import time
import logging
import logging.config
from bson.codec_options import CodecOptions
from datetime import datetime, timedelta
import pytz
import copy

try:
    import pickle as pickle
except ImportError:
    import pickle

import arrow
import pymongo
import pymongo.errors
import numpy as np
import pandas as pd

from vnpy.trader.vtConstant import *
from vnpy.trader.vtGlobal import globalSetting, settingFilePath
from vnpy.trader.vtObject import VtTickData, VtBarData, VtContractData, VtMarginRate, VtCommissionRate
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine as VTBacktestingEngine
from vnpy.trader.app.ctaStrategy.ctaBacktesting import TradingResult, formatNumber, DailyResult
from vnpy.trader.vtFunction import getTempPath, getJsonPath, LOCAL_TIMEZONE
from vnpy.trader.vtGateway import VtOrderData, VtTradeData
from .ctaBase import *


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

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # 本地停止单
        self.log = logging.getLogger('ctabacktesting')

        super(BacktestingEngine, self).__init__()

        self.vtContract = None  # 合约详情 VtConcractData
        self.vtMarginRate = None  # 保证金率
        self.vtCommissionRate = None  # 手续费率

        self.datas = []  # 一个合约的全部基础数据，tick , 1min bar OR 1day bar

        self.marginRate = None  # 保证金比例对象 VtMarginRate()
        self.margin = None # 最后保证金

        self.isShowFig = True  # 回测后输出结果时是否展示图片
        self.isOutputResult = True  # 回测后输出结果时是否展示图片
        self.dailyResult = OrderedDict()  # 按日汇总的回测结果
        self.tradeResult = OrderedDict()  # 按笔汇总回测结果

        self.loadHised = False  # 是否已经加载过了历史数据
        self.barPeriod = '1T'  # 默认是1分钟 , 15T 是15分钟， 1H 是1小时，1D 是日线

        self.collectionName = MINUTE_COL_NAME
        self.initMongoDB()

        logging.Formatter.converter = self.barTimestamp

    # ------------------------------------------------
    # 通用功能
    # ------------------------------------------------

    # ----------------------------------------------------------------------
    def roundToPriceTick(self, price):
        """取整价格到合约最小价格变动"""
        if not self.priceTick:
            return price

        newPrice = round(price / self.priceTick, 0) * self.priceTick
        return newPrice

    # #----------------------------------------------------------------------
    # def output(self, content):
    #     """输出内容"""
    #     self.log.warning(content)

    # ------------------------------------------------



    # def resample(self):
    #     """
    #     聚合数据
    #     :return:
    #     """
    #
    #     self.initData = self._resample(self.barPeriod, self._initData)
    #     self.datas = self._resample(self.barPeriod, self._datas)
    def initMongoDB(self):
        print(('globalSetting: {}'.format(settingFilePath)))
        self.dbClient = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'],
                                            connectTimeoutMS=500)

        ctpdb = self.dbClient[globalSetting['mongoCtpDbn']]
        ctpdb.authenticate(globalSetting['mongoUsername'], globalSetting['mongoPassword'])

        # 1min bar collection
        self.ctpCol1minBar = ctpdb['bar_1min'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 日线的 collection
        self.ctpCol1dayBar = ctpdb['bar_1day'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 合约详情 collection
        self.ctpColContract = ctpdb['contract'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

    def closeMongoDB(self):
        self.dbClient.close()

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
    # ------------------------------------------------

    # ----------------------------------------------------------------------
    # def setStartDate(self, startDate='20100416', initDays=10):
    #     """设置回测的启动日期"""
    #     self.startDate = startDate
    #     self.initDays = initDays
    #
    #     self.dataStartDate = LOCAL_TIMEZONE.localize(datetime.strptime(startDate, '%Y%m%d'))
    #
    #     # initTimeDelta = timedelta(initDays)
    #     # 要获取 initDays 个交易日的数据
    #     sql = {
    #         'symbol': self.symbol,
    #         'tradingDay': {
    #             '$gte': self.dataStartDate,
    #         }
    #     }
    #     cursor = self.ctpCol1dayBar.find(sql, {'_id': 0})
    #     # 顺序排列
    #     cursor.sort('tradingDay')
    #
    #     cursor.skip(initDays)
    #     dayBar = cursor.next()
    #
    #     self.strategyStartDate = dayBar['tradingDay']
    #
    #     self.log.warning(u'strategyStartDate {}'.format(str(self.strategyStartDate)))


    def setShowFig(self, isShow):
        """
        回测后是否展示图片
        :param isShow:
        :return:
        """
        self.isShowFig = isShow

    def setOutputResult(self, isOutputResult):
        """
        回测后是否展示图片
        :param isShow:
        :return:
        """
        self.isOutputResult = isOutputResult

    def setStartDate(self, startDate=None, initDays=None):
        """
        设置回测的启动日期
        :param startDate: 策略的启动日期
        :param initDays: 该参数作废
        :return:
        """
        if isinstance(startDate, datetime):
            self.startDate = startDate
        elif isinstance(startDate, str) or isinstance(startDate, str):
            self.startDate = arrow.get(startDate).datetime
        else:
            err = '未知的回测起始日期 {}'.format(str(startDate))
            self.log.critical(err)
            raise ValueError(err)

        if self.startDate.strftime('%H:%M:%S.%f') != '00:00:00.000000':
            msg = 'startdate 必须为一个零点的日期'
            self.log.critical(msg)
            raise ValueError(msg)

        self.dataStartDate = self.strategyStartDate = self.startDate

        # self.startDate
        # self.strategyStartDate
        # self.dataStartDate

    def setEndDate(self, endDate=''):
        """设置回测的结束日期"""
        self.endDate = endDate

        if isinstance(endDate, datetime):
            self.endDate = endDate
        elif isinstance(endDate, str) or isinstance(endDate, str):
            self.endDate = arrow.get(endDate).datetime
        else:
            err = '未知的回测结束日期 {}'.format(str(endDate))
            self.log.critical(err)
            raise ValueError(err)

        if self.endDate.strftime('%H:%M:%S.%f') != '00:00:00.000000':
            msg = 'endDate 必须为一个零点的日期'
            self.log.critical(msg)
            raise ValueError(msg)

    def setBarPeriod(self, barPeriod):
        """

        :return:
        """
        periodTypes = ['T', 'H', 'D']
        if barPeriod[-1] not in periodTypes:
            raise ValueError('周期应该为 {} , 如 15T 是15分钟K线这种格式'.format(str(periodTypes)))

        self.barPeriod = barPeriod

    def setMarginRate(self, marginRate):
        """
        设置保证金比例
        :param margin:
        :return:
        """
        self.marginRate = marginRate
        assert isinstance(marginRate, VtMarginRate)

    def setSymbol(self, vtSymbol):
        """
        设置引擎使用的合约
        会自动从合约详情里面加载相关的基础数据
        :param vtSymbol:
        :return:
        """
        self.symbol = vtSymbol
        sql = {
            'vtSymbol': self.symbol
        }
        contractDic = self.ctpColContract.find_one(sql, {'_id': 0})

        # 合约详情
        self.vtContract = vtCon = VtContractData()
        # 保证金率
        self.vtMarginRate = vtMar = VtMarginRate()
        # 手续费率
        vtCom = VtCommissionRate()

        for k, v in list(contractDic.items()):
            if hasattr(vtCon, k):
                setattr(vtCon, k, v)

            if hasattr(vtMar, k):
                setattr(vtMar, k, v)

            if hasattr(vtCom, k):
                setattr(vtCom, k, v)

        if vtCon.exchange == 'SHFE':
            vtCom.closeTodayRatioByMoney = vtCom.closeRatioByMoney
            vtCom.closeTodayRatioByVolume = vtCom.closeRatioByVolume

        # 设置数据库
        # self.setDatabase(MINUTE_DB_NAME, self.vtSymbol)
        startDate = contractDic['activeStartDate']
        endDate = contractDic['activeEndDate']
        if startDate is None:
            err = '{} 不是主力合约'.format(self.symbol)
            self.log.error(err)
            raise ValueError(err)
        self.setStartDate(startDate)  # 设置回测用的数据起始日期
        if endDate:
            self.setEndDate(endDate)  # 设置回测用的数据起始日期

        # 默认都是1滑点
        if self.slippage == 0:
            self.setSlippage(1)

        # 设置手续费
        self.setRate(vtCom)
        assert isinstance(self.rate, VtCommissionRate)

        # 一手的大小
        self.setSize(vtCon.size)
        self.setPriceTick(vtCon.priceTick)  # 设置股指最小价格变动
        self.setMarginRate(vtMar)

        # 回测时调高保证金比例
        vtMar.backtestingRate = 1.2
        vtMar.marginRate *= vtMar.backtestingRate

        assert isinstance(self.marginRate, VtMarginRate)

    # ------------------------------------------------
    # 数据回放相关
    # ------------------------------------------------

    # ----------------------------------------------------------------------
    def loadHistoryData(self):
        """载入历史数据"""
        self.loadHised = True
        collection = {
            MINUTE_COL_NAME: self.ctpCol1minBar,
            DAY_COL_NAME: self.ctpCol1dayBar,
        }.get(self.collectionName)

        self.log.info('开始载入数据')

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = VtBarData
        else:
            dataClass = VtTickData

        # 载入初始化需要用的数据
        flt = {'symbol': self.symbol}

        initCursor = collection.find(flt, {'_id': 0})
        initCount = initCursor.count()
        self.log.info('预计加载数据 {}'.format(initCount))

        # 将数据从查询指针中读取出，并生成列表
        self.datas = []  # 清空initData列表
        for d in initCursor:
            data = dataClass()
            data.load(d)
            self.datas.append(data)

        # 对 datetime 排序
        self.datas.sort(key=lambda d: d.datetime)

        self.clearBeforeBar()

        self.log.info('载入完成')

    def clearBeforeBar(self):
        """
        剔除 9:00 和 21:00 的不合理K线
        :return:
        """
        if self.mode == self.BAR_MODE and self.collectionName == MINUTE_COL_NAME:
            timestamp = ((9, 0, 0), (21, 0, 0))
            preKline = None
            for k in self.datas[:]:
                if (k.datetime.hour, k.datetime.minute, k.datetime.second) in timestamp:
                    # 取出提前开始的K线 9:00
                    preKline = k
                    self.datas.remove(k)
                if preKline:
                    # 聚合到下一根K线上，可能是 9:02 ，不一定是9:01
                    k.high = max(preKline.high, k.high)
                    k.low = min(preKline.low, k.low)
                    k.volume += preKline.volume
                    preKline = None
            self.log.info('剔除过早的K线')

    # ----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        # 载入历史数据
        if not self.loadHised:
            self.loadHistoryData()

        # 聚合数据
        # self.resample()

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = VtBarData
            func = self.newBar
        else:
            dataClass = VtTickData
            func = self.newTick

        self.log.info('开始回测')

        self.strategy.inited = True
        self.strategy.onInit()
        self.log.info('策略初始化完成')

        self.dt = self.datas[0].datetime

        self.strategy.trading = True
        self.strategy.onStart()
        self.log.info('策略启动完成')

        self.log.info('开始回放数据')

        for data in self.datas:
            td = data.tradingDay
            if self.endDate and self.endDate < td:
                # 要回测的时间段结束
                break
            if self.strategyStartDate <= td:
                try:
                    func(data)
                except:
                    self.log.error('异常 bar: {}'.format(data.datetime))
                    raise

        self.log.info('数据回放结束')
        self.strategy.trading = False

    def loadBar(self, symbol, collectionName, barNum, barPeriod=1):
        """直接返回初始化数据列表中的Bar"""
        initDatas = []
        needBarNum = barPeriod * barNum

        # 从策略起始日之前开始加载数据
        for b in self.datas:
            if b.tradingDay < self.strategyStartDate:
                initDatas.append(b)
            else:
                # 加载完成
                break
        # 只返回指定数量的 bar
        initDataNum = len(initDatas)

        if initDataNum < needBarNum:
            self.log.info('{} 预加载的 bar 数量 {} != barAmount:{}'.format(symbol, initDataNum, needBarNum))
            return initDatas

        # 获得余数，这里一个 bar 不能从一个随意的地方开始，要从头开始计数
        barAmount = initDataNum % barPeriod + needBarNum

        return initDatas[-barAmount:]

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

        def _crossStopOrder(so):
            """

            :param so:
            :return: bool(是否成交)
            """
            stopOrderID = so.stopOrderID
            # 判断是否会成交
            if so.stopProfile:
                buyCross = so.direction == DIRECTION_LONG and sellCrossPrice <= so.price
                sellCross = so.direction == DIRECTION_SHORT and buyCrossPrice >= so.price
            else:
                buyCross = so.direction == DIRECTION_LONG and buyCrossPrice >= so.price
                sellCross = so.direction == DIRECTION_SHORT and sellCrossPrice <= so.price

            # 如果发生了成交
            if not (buyCross or sellCross):
                return False
            else:
                # 更新停止单状态，并从字典中删除该停止单
                so.status = STOPORDER_TRIGGERED
                if stopOrderID in self.workingStopOrderDict:
                    del self.workingStopOrderDict[stopOrderID]

                if so.volume == 0:
                    # 下单量为0的话，不做限价撮合
                    self.strategy.onStopOrder(so)
                    return True
                else:
                    # 要做撮合前，先将 vtOrder
                    orderID = str(self.limitOrderCount)
                    so.vtOrderID = orderID
                    self.strategy.onStopOrder(so)

                # 推送成交数据
                self.tradeCount += 1  # 成交编号自增1
                tradeID = str(self.tradeCount)
                trade = VtTradeData()
                trade.vtSymbol = so.vtSymbol
                trade.tradeID = tradeID
                trade.vtTradeID = tradeID
                trade.tradingDay = self.bar.tradingDay

                if buyCross:
                    self.strategy.pos += so.volume
                    if so.stopProfile:
                        trade.price = min(bestCrossPrice, so.price)
                    else:
                        trade.price = max(bestCrossPrice, so.price)
                else:
                    self.strategy.pos -= so.volume
                    if so.stopProfile:
                        trade.price = max(bestCrossPrice, so.price)
                    else:
                        trade.price = min(bestCrossPrice, so.price)

                self.limitOrderCount += 1
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
                self.strategy.onOrder(order)
                self.strategy.onTrade(trade)
                return True

        # 遍历停止单字典中的所有停止单
        # for stopOrderID, so in self.workingStopOrderDict.items():
        stopOrders = self.getAllStopOrdersSorted()
        count = 0
        isCrossed = False
        while count < 100:
            count += 1
            for so in stopOrders:
                if so.status == STOPORDER_WAITING:
                    # 等待中的才进行撮合
                    isCrossed = _crossStopOrder(so)
                else:
                    if so.stopOrderID in self.workingStopOrderDict:
                        self.log.warning('异常的停止单抛弃 {}'.format(so))
                        del self.workingStopOrderDict[so.stopOrderID]
                if isCrossed:
                    self.log.info('撮合成功 {}'.format(so))
                    # 出现成交，重新整理停止单队列
                    preStopOrders, stopOrders = stopOrders, self.getAllStopOrdersSorted()
                    # 新的开仓单不加入
                    stopOrders = [so for so in stopOrders if
                                  not (so not in preStopOrders and so in stopOrders and so.offset == OFFSET_OPEN)]
                    self.log.info('再次撮合 {}'.format(len(stopOrders)))
                    break
            if isCrossed:
                isCrossed = False
            else:
                break

        if count >= 100:
            self.log.warning('订单量过大 {}'.format(count))

    def getAllStopOrdersSorted(self):
        """
        对全部停止单排序后
        :return:
        """
        longOpenStopOrders = []
        shortCloseStopOrders = []
        shortOpenStopOrders = []
        longCloseStopOrders = []
        stopOrders = []
        for so in list(self.workingStopOrderDict.values()):
            if so.direction == DIRECTION_LONG:
                if so.offset == OFFSET_OPEN:
                    # 买开
                    longOpenStopOrders.append(so)
                else:
                    # 卖空
                    shortCloseStopOrders.append(so)
            elif so.direction == DIRECTION_SHORT:
                if so.offset == OFFSET_OPEN:
                    # 卖开
                    shortOpenStopOrders.append(so)
                else:
                    # 买空
                    longCloseStopOrders.append(so)
            else:
                stopOrders.append(so)
                self.log.error('未知的停止单方向 {}'.format(so.direction))

        # 根据触发价排序，优先触发更优的
        # 买开
        longOpenStopOrders.sort(key=lambda so: (so.price, so.priority))
        # 平多
        shortCloseStopOrders.sort(key=lambda so: (so.price, -so.priority))
        # 开多
        shortOpenStopOrders.sort(key=lambda so: (so.price, -so.priority))
        shortOpenStopOrders.reverse()
        # 卖空
        longCloseStopOrders.sort(key=lambda so: (so.price, so.priority))
        longCloseStopOrders.reverse()

        # 先撮合平仓单
        if self.bar.open >= self.bar.close:
            # 阴线，撮合优先级 平仓单 > 多单
            stopOrders.extend(shortCloseStopOrders)
            stopOrders.extend(longCloseStopOrders)
            stopOrders.extend(longOpenStopOrders)
            stopOrders.extend(shortOpenStopOrders)
        else:
            # 阳线，撮合优先级，平仓单 > 空单
            stopOrders.extend(longCloseStopOrders)
            stopOrders.extend(shortCloseStopOrders)
            stopOrders.extend(shortOpenStopOrders)
            stopOrders.extend(longOpenStopOrders)

        return stopOrders

    def getOrder(self, vtOrderID):
        vtOrder = self.limitOrderDict.get(vtOrderID)
        if vtOrder is None:
            vtOrder = self.stopOrderDict.get(vtOrderID)
        return vtOrder

    # ----------------------------------------------------------------------
    def calculateDailyResult(self):
        """计算按日统计的交易结果"""

        # 将成交添加到每日交易结果中
        for trade in list(self.tradeDict.values()):
            date = trade.tradingDay.date()
            dailyResult = self.dailyResultDict[date]
            dailyResult.addTrade(trade)

        # 遍历计算每日结果
        previousClose = 0
        openPosition = 0
        for dailyResult in list(self.dailyResultDict.values()):
            dailyResult.previousClose = previousClose
            previousClose = dailyResult.closePrice

            dailyResult.calculatePnl(openPosition, self.size, self.strategy.getCommission, self.slippage,
                                     self.marginRate)
            openPosition = dailyResult.closePosition
            self.margin = dailyResult.closePrice * self.size * self.marginRate.marginRate

        # 生成DataFrame
        resultDict = {k: [] for k in list(DailyResult(None, None).__dict__.keys())}
        for dailyResult in list(self.dailyResultDict.values()):
            for k, v in list(dailyResult.__dict__.items()):
                resultDict[k].append(v)

        resultDf = pd.DataFrame.from_dict(resultDict)

        # 计算衍生数据
        resultDf = resultDf.set_index('date')

        return resultDf

    # ----------------------------------------------------------------------
    def showDailyResult(self, df=None):
        """显示按日统计的交易结果"""
        if not df:
            df = self.calculateDailyResult()

        assert isinstance(df, pd.DataFrame)

        df['balance'] = df['netPnl'].cumsum() + self.capital
        df['return'] = (np.log(df['balance']) - np.log(df['balance'].shift(1))).fillna(0)
        df['highlevel'] = df['balance'].rolling(min_periods=1, window=len(df), center=False).max()
        df['drawdown'] = df['balance'] - df['highlevel']
        df['drawdownPer'] = df['drawdown'] / df['highlevel']
        df['marginPer'] = df['margin'] / df['balance']

        if __debug__:
            self.df = df

        # 计算统计结果
        startDate = df.index[0]
        endDate = df.index[-1]

        totalDays = len(df)
        profitDays = len(df[df['netPnl'] > 0])
        lossDays = len(df[df['netPnl'] < 0])

        endBalance = df['balance'].iloc[-1]
        maxDrawdown = df['drawdown'].min()
        maxDrawdownPer = df['drawdownPer'].min()

        totalNetPnl = df['netPnl'].sum()
        dailyNetPnl = totalNetPnl / totalDays

        totalCommission = df['commission'].sum()
        dailyCommission = totalCommission / totalDays

        totalSlippage = df['slippage'].sum()
        dailySlippage = totalSlippage / totalDays

        totalTurnover = df['turnover'].sum()
        dailyTurnover = totalTurnover / totalDays

        totalTradeCount = int(df['tradeCount'].sum())
        dailyTradeCount = totalTradeCount / totalDays

        totalReturn = (endBalance / self.capital - 1)
        dailyReturn = df['return'].mean() * 100
        returnStd = df['return'].std()

        maxMarginPer = df['marginPer'].max()

        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(240)
        else:
            sharpeRatio = 0

        # 输出统计结果

        self.dailyResult['首个交易日'] = startDate
        self.dailyResult['最后交易日'] = endDate

        self.dailyResult['总交易日'] = totalDays
        self.dailyResult['盈利交易日'] = profitDays
        self.dailyResult['亏损交易日'] = lossDays

        self.dailyResult['起始资金'] = self.capital
        self.dailyResult['结束资金'] = endBalance
        self.dailyResult['最大保证金占用率'] = maxMarginPer
        self.dailyResult['保证金'] = self.margin

        self.dailyResult['总收益率'] = totalReturn
        self.dailyResult['总盈亏'] = totalNetPnl
        self.dailyResult['日最大回撤'] = maxDrawdown
        self.dailyResult['日最大回撤率'] = maxDrawdownPer

        self.dailyResult['总手续费'] = float(totalCommission)
        self.dailyResult['总滑点'] = float(totalSlippage)
        self.dailyResult['总成交金额'] = totalTurnover
        self.dailyResult['总成交笔数'] = totalTradeCount

        self.dailyResult['日均盈亏'] = dailyNetPnl
        self.dailyResult['日均手续费'] = dailyCommission
        self.dailyResult['日均滑点'] = dailySlippage
        self.dailyResult['日均成交金额'] = dailyTurnover
        self.dailyResult['日均成交笔数'] = dailyTradeCount

        self.dailyResult['日均收益率'] = dailyReturn
        self.dailyResult['收益标准差'] = returnStd
        self.dailyResult['夏普率'] = sharpeRatio

        if self.isOutputResult:
            self.printResult(self.dailyResult)

        self.dailyResult['techIndBar'] = self.strategy.techIndBar
        self.dailyResult['techIndLine'] = self.strategy.techIndLine

        # 收益率曲线
        self.dailyResult['netPnl'] = list(df['netPnl'])
        self.dailyResult['netPnlp'] = list(df['netPnlp'])

        balanceList = [self.capital] + list(df['balance'].values)
        balanceList = pd.Series(balanceList).pct_change()
        self.dailyResult['日收益率'] = balanceList.values[1:].tolist()
        self.dailyResult['结算日'] = [d.value for d in pd.to_datetime(df.index)]

        assert len(self.dailyResult['netPnl']) == len(self.dailyResult['结算日'])

        if not self.isShowFig:
            return

        import matplotlib.pyplot as plt

        # 绘图
        fig = plt.figure(figsize=(10, 16))

        subPlotCount = 0
        subPlotNum = 8

        subPlotCount += 1
        pBalance = plt.subplot(subPlotNum, 1, subPlotCount)
        pBalance.set_title('Balance {}'.format(self.symbol))
        df['balance'].plot(legend=True, grid=True)

        subPlotCount += 1
        pBalance = plt.subplot(subPlotNum, 1, subPlotCount)
        pBalance.set_title('Daily Pnl Cumsum {}'.format(self.symbol))
        df['netPnl'].cumsum().plot(legend=True, grid=True)
        
        subPlotCount += 1
        pBalance = plt.subplot(subPlotNum, 1, subPlotCount)
        pBalance.set_title('Daily Pnlp Cumsum {}'.format(self.symbol))
        df['netPnlp'].cumsum().plot(legend=True, grid=True)

        subPlotCount += 1
        pDrawdown = plt.subplot(subPlotNum, 1, subPlotCount)
        pDrawdown.set_title('Drawdown')
        pDrawdown.grid(True, color='gray')
        pDrawdown.fill_between(df['drawdown'].index, df['drawdown'].values)

        subPlotCount += 1
        pDrawdownPer = plt.subplot(subPlotNum, 1, subPlotCount)
        pDrawdownPer.set_title('DrawdownPer')
        pDrawdownPer.grid(True, color='gray')
        pDrawdownPer.fill_between(df['drawdownPer'].index, df['drawdownPer'].values)

        subPlotCount += 1
        pPnl = plt.subplot(subPlotNum, 1, subPlotCount)
        pPnl.set_title('Daily Pnl')
        df['netPnl'].plot(kind='bar', legend=False, grid=False, xticks=[])

        subPlotCount += 1
        pMp = plt.subplot(subPlotNum, 1, subPlotCount)
        pMp.set_title('Daily MarginPer')
        df['marginPer'].plot(kind='bar', legend=False, grid=False, xticks=[])

        subPlotCount += 1
        pKDE = plt.subplot(subPlotNum, 1, subPlotCount)
        pKDE.set_title('Daily Pnl Distribution')
        df['netPnl'].hist(bins=50)

        if __debug__:
            plt.show()

    def printResult(self, result):
        """

        :param result: {}
        :return:
        """
        if result is self.dailyResult:
            print(('-' * 30))
            print(('{} 计算按日统计结果'.format(self.symbol)))
            print(('-' * 30))
        elif result is self.tradeResult:
            print(('-' * 30))
            print(('{} 逐笔计算回测结果'.format(self.symbol)))
            print(('-' * 30))

        for k, v in list(result.items()):
            if isinstance(v, dict) or isinstance(v, list):
                continue
            if isinstance(v, float) or isinstance(v, int):
                if '率' in k and k not in ('夏普率'):
                    v *= 100
                    v = formatNumber(v)
                    v += '%'
                else:
                    v = formatNumber(v)

            print(('%s：\t%s' % (k, v)))

    def calculateBacktestingResult(self):
        """
        计算回测结果
        """

        # 首先基于回测后的成交记录，计算每笔交易的盈亏
        resultList = []  # 交易结果列表

        longTrade = []  # 未平仓的多头交易
        shortTrade = []  # 未平仓的空头交易

        tradeTimeList = []  # 每笔成交时间戳
        posList = [0]  # 每笔成交后的持仓情况

        for trade in list(self.tradeDict.values()):
            # 复制成交对象，因为下面的开平仓交易配对涉及到对成交数量的修改
            # 若不进行复制直接操作，则计算完后所有成交的数量会变成0
            trade = copy.copy(trade)

            # 多头交易
            if trade.direction == DIRECTION_LONG:
                # 如果尚无空头交易
                if not shortTrade:
                    longTrade.append(trade)
                # 当前多头交易为平空
                else:
                    while True:
                        exitTrade = trade
                        for t in shortTrade:
                            if t.volume == exitTrade.volume:
                                break
                        entryTrade = t

                        # 清算开平仓交易
                        closedVolume = min(exitTrade.volume, entryTrade.volume)
                        result = TradingResult(entryTrade.price, entryTrade.dt,
                                               exitTrade.price, exitTrade.dt,
                                               -closedVolume, self.strategy.getCommission, self.slippage, self.size)
                        resultList.append(result)

                        posList.extend([-1, 0])
                        tradeTimeList.extend([result.entryDt, result.exitDt])

                        # 计算未清算部分
                        entryTrade.volume -= closedVolume
                        exitTrade.volume -= closedVolume

                        # 如果开仓交易已经全部清算，则从列表中移除
                        if not entryTrade.volume:
                            shortTrade.remove(entryTrade)

                        # 如果平仓交易已经全部清算，则退出循环
                        if not exitTrade.volume:
                            break

                        # 如果平仓交易未全部清算，
                        if exitTrade.volume:
                            # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                            # 等于新的反向开仓交易，添加到队列中
                            if not shortTrade:
                                longTrade.append(exitTrade)
                                break
                            # 如果开仓交易还有剩余，则进入下一轮循环
                            else:
                                pass

            # 空头交易
            else:
                # 如果尚无多头交易
                if not longTrade:
                    shortTrade.append(trade)
                # 当前空头交易为平多
                else:
                    while True:
                        exitTrade = trade
                        for t in longTrade:
                            if t.volume == exitTrade.volume:
                                break
                        entryTrade = t

                        # 清算开平仓交易
                        closedVolume = min(exitTrade.volume, entryTrade.volume)
                        result = TradingResult(entryTrade.price, entryTrade.dt,
                                               exitTrade.price, exitTrade.dt,
                                               closedVolume, self.strategy.getCommission, self.slippage, self.size)
                        resultList.append(result)

                        posList.extend([1, 0])
                        tradeTimeList.extend([result.entryDt, result.exitDt])

                        # 计算未清算部分
                        entryTrade.volume -= closedVolume
                        exitTrade.volume -= closedVolume

                        # 如果开仓交易已经全部清算，则从列表中移除
                        if not entryTrade.volume:
                            longTrade.remove(entryTrade)

                        # 如果平仓交易已经全部清算，则退出循环
                        if not exitTrade.volume:
                            break

                        # 如果平仓交易未全部清算，
                        if exitTrade.volume:
                            # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                            # 等于新的反向开仓交易，添加到队列中
                            if not longTrade:
                                shortTrade.append(exitTrade)
                                break
                            # 如果开仓交易还有剩余，则进入下一轮循环
                            else:
                                pass

                                # 到最后交易日尚未平仓的交易，则以最后价格平仓
        if self.mode == self.BAR_MODE:
            endPrice = self.bar.close
        else:
            endPrice = self.tick.lastPrice

        for trade in longTrade:
            result = TradingResult(trade.price, trade.dt, endPrice, self.dt,
                                   trade.volume, self.strategy.getCommission, self.slippage, self.size)
            resultList.append(result)

        for trade in shortTrade:
            result = TradingResult(trade.price, trade.dt, endPrice, self.dt,
                                   -trade.volume, self.strategy.getCommission, self.slippage, self.size)
            resultList.append(result)

            # 检查是否有交易
        if not resultList:
            self.log.info('无交易结果')
            return {}

        # resultList.sort(key=lambda r: r.datetime)

        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等
        capital = self.capital  # 资金
        maxCapital = capital  # 资金最高净值
        drawdown = 0  # 回撤

        totalResult = 0  # 总成交数量
        totalTurnover = 0  # 总成交金额（合约面值）
        totalCommission = 0  # 总手续费
        totalSlippage = 0  # 总滑点

        timeList = []  # 时间序列
        pnlList = []  # 每笔盈亏序列
        pnlpList = []  # 每笔盈亏率序列
        balanceList = []  # 盈亏汇总的时间序列
        capitalList = []  # 资金时间序列
        drawdownList = []  # 回撤的时间序列
        # drawdownPerList = []  # 回撤比率的时间序列
        # drawdownRatePerTradeList = []  # 单笔最大回撤率
        posList = []  # 仓位变化
        marginList = []  # 保证金
        marginRateList = []  # 保证金占用比例

        winningResult = 0  # 盈利次数
        losingResult = 0  # 亏损次数
        totalWinning = 0  # 总盈利金额
        totalLosing = 0  # 总亏损金额
        # pos = 0  # 总体持仓情况

        for result in resultList:
            margin = abs(result.volume * self.size * result.entryPrice * self.marginRate.marginRate)
            marginRate = margin / capital
            capital += result.pnl  # pnl 已经扣掉了滑点和手续费
            maxCapital = max(capital, maxCapital)
            drawdown = capital - maxCapital
            # drawdownRatePerTrade = min(0, result.pnl / margin)  # 单笔最大回撤率

            pnlList.append(result.pnl)
            pnlpList.append(result.pnlp)
            timeList.append(result.exitDt)  # 交易的时间戳使用平仓时间
            capitalList.append(capital)
            drawdownList.append(drawdown)
            # drawdownPerList.append(drawdown / maxCapital)
            posList.append(result.volume)
            marginList.append(margin)
            marginRateList.append(marginRate)
            # drawdownRatePerTradeList.append(drawdownRatePerTrade)

            totalResult += 1
            totalTurnover += abs(result.turnover)
            totalCommission += result.commission
            totalSlippage += result.slippage

            if result.pnl >= 0:
                winningResult += 1
                totalWinning += result.pnl
            else:
                losingResult += 1
                totalLosing += result.pnl

        # 计算盈亏相关数据
        winningRate = winningResult / totalResult  # 胜率

        averageWinning = 0  # 这里把数据都初始化为0
        averageLosing = 0
        profitLossRatio = 0

        if winningResult:
            averageWinning = totalWinning / winningResult  # 平均每笔盈利
        if losingResult:
            averageLosing = totalLosing / losingResult  # 平均每笔亏损
        if averageLosing:
            profitLossRatio = -averageWinning / averageLosing  # 盈亏比

        # 返回回测结果
        d = {}
        d['capital'] = capital - self.capital
        d['maxCapital'] = maxCapital
        d['drawdown'] = drawdown
        # d['maxDrawdownPer'] = abs(min(drawdownPerList))
        d['totalResult'] = totalResult
        d['totalTurnover'] = totalTurnover
        d['totalCommission'] = totalCommission
        d['totalSlippage'] = totalSlippage
        d['timeList'] = timeList
        d['pnlList'] = pnlList
        d['pnlpList'] = pnlpList
        d['capitalList'] = capitalList
        d['drawdownList'] = drawdownList
        # d['drawdownPerList'] = drawdownPerList
        d['winningRate'] = winningRate
        d['winningResult'] = winningResult
        d['losingResult'] = losingResult
        d['totalWinning'] = totalWinning
        d['totalLosing'] = totalLosing
        d['winLoseRate'] = - totalWinning / totalLosing if 0 != totalLosing else 0
        d['averageWinning'] = averageWinning
        d['averageLosing'] = averageLosing
        d['profitLossRatio'] = profitLossRatio
        d['posList'] = posList
        d['marginList'] = marginList
        d['marginRateList'] = marginRateList
        d['tradeTimeList'] = tradeTimeList
        d['resultList'] = resultList
        # d['drawdownRatePerTradeList'] = drawdownRatePerTradeList

        return d

    # ----------------------------------------------------------------------
    def showBacktestingResult(self):
        """显示回测结果"""
        d = self.calculateBacktestingResult()
        if __debug__:
            self.d = d
        # 输出
        if not d:
            self.log.info('没有回测结果')
            return

        self.tradeResult['第一笔交易'] = d['timeList'][0]
        self.tradeResult['最后一笔交易'] = d['timeList'][-1]
        self.tradeResult['总交易次数'] = d['totalResult']  # 1次可以N手

        self.tradeResult['初始金'] = self.capital
        self.tradeResult['总盈亏'] = d['capital']
        self.tradeResult['总手续费'] = float(d['totalCommission'])
        self.tradeResult['总滑点'] = float(d['totalSlippage'])
        self.tradeResult['交易成本'] = d['totalSlippage'] + d['totalCommission']
        self.tradeResult['纯盈亏'] = self.tradeResult['总盈亏'] + self.tradeResult['交易成本']
        self.tradeResult['成本比例'] = 1 if self.tradeResult['纯盈亏'] == 0 else self.tradeResult['交易成本'] / self.tradeResult['纯盈亏']
        self.tradeResult['盈利次数'] = d['winningResult']
        self.tradeResult['亏损次数'] = d['losingResult']
        self.tradeResult['总盈利'] = d['totalWinning']
        self.tradeResult['总亏损'] = d['totalLosing']

        self.tradeResult['平均每笔盈亏'] = d['capital'] / d['totalResult']
        self.tradeResult['平均每笔滑点'] = d['totalSlippage'] / d['totalResult']
        self.tradeResult['平均每笔佣金'] = d['totalCommission'] / d['totalResult']
        self.tradeResult['平均每笔保证金'] = sum(d['marginList']) / len(d['marginList'])

        self.tradeResult['最大回撤'] = min(d['drawdownList'])
        # self.tradeResult[u'单笔最大回撤率1'] = min(d['drawdownRatePerTradeList'])
        self.tradeResult['单笔最大回撤率'] = min(d['pnlpList'])

        self.tradeResult['单笔最大回撤'] = min(d['pnlList'])
        # self.tradeResult[u'最大回撤率'] = self.tradeResult[u'最大回撤'] / self.tradeResult[u'平均每笔保证金']
        pnlp = pd.Series(d['pnlpList']).cumsum()
        maxPnlp = pnlp.cummax()
        dropdown = pnlp - maxPnlp
        self.tradeResult['最大回撤率'] = min(dropdown)

        self.tradeResult['胜率'] = d['winningRate']
        # self.tradeResult[u'收益率'] = self.tradeResult[u'总盈亏'] / self.tradeResult[u'平均每笔保证金']
        self.tradeResult['收益率'] = d['pnlpList'][-1]
        self.tradeResult['盈利交易平均值'] = d['averageWinning']
        self.tradeResult['亏损交易平均值'] = d['averageLosing']
        self.tradeResult['盈亏比'] = d['profitLossRatio']
        self.tradeResult['总盈亏比'] = d['winLoseRate']

        if self.isOutputResult:
            self.printResult(self.tradeResult)

        # 收益率曲线
        balanceList = [self.capital] + d['capitalList']
        balanceList = pd.Series(balanceList).pct_change()
        self.tradeResult['收益率曲线'] = list(balanceList.values[1:])
        self.tradeResult['成交单'] = [r.toReutlDB() for r in d['resultList']]
        self.tradeResult['pnl'] = d['pnlList']
        self.tradeResult['pnlp'] = d['pnlpList']

        if not self.isShowFig:
            return

        import matplotlib.pyplot as plt
        
        # 绘图
        fig = plt.figure(figsize=(10, 16))

        subplotNum = 3
        subplotCount = 0

        subplotCount += 1
        balance = plt.subplot(subplotNum, 1, subplotCount)
        balance.set_ylabel("balance")
        balance.grid(True, color='gray')
        balance.plot(pd.Series(d['pnlList']).cumsum().values, color='r', lw=0.8)

        # subplotCount += 1
        # pCapital = plt.subplot(subplotNum, 1, subplotCount)
        # pCapital.set_ylabel("capital")
        # pCapital.grid(True, color='gray')
        # pCapital.plot(d['capitalList'], color='r', lw=0.8)

        subplotCount += 1
        pDD = plt.subplot(subplotNum, 1, subplotCount)
        pDD.set_ylabel("DD")
        pDD.grid(True, color='gray')
        pDD.bar(list(range(len(d['drawdownList']))), d['drawdownList'], color='g')

        # subplotCount += 1
        # pDD = plt.subplot(subplotNum, 1, subplotCount)
        # pDD.set_ylabel("DDp")
        # pDD.grid(True, color='gray')
        # pDD.bar(range(len(dropdown)), list(dropdown), color='g')

        # subplotCount += 1
        # pDDp = plt.subplot(subplotNum, 1, subplotCount)
        # pDDp.set_ylabel("DDP")
        # pDDp.bar(range(len(d['drawdownPerList'])), d['drawdownPerList'], color='g')

        subplotCount += 1
        pPnl = plt.subplot(subplotNum, 1, subplotCount)
        pPnl.set_ylabel("pnl")
        pPnl.hist(d['pnlList'], bins=50, color='c')

        # subplotCount += 1
        # pPosition = plt.subplot(subplotNum, 1, subplotCount)
        # pPosition.set_ylabel("Position")
        # pPosition.bar(range(len(d['posList'])), d['posList'], color='g')

        # # 策略中记录的持仓变化
        # subplotCount += 1
        # sPos = plt.subplot(subplotNum, 1, subplotCount)
        # sPos.set_ylabel("sPos")
        # sPos.bar(range(len(self.strategy.posList)), self.strategy.posList, color='g')

        # subplotCount += 1
        # pMargin = plt.subplot(subplotNum, 1, subplotCount)
        # pMargin.set_ylabel("Margin")
        # pMargin.bar(range(len(d['marginList'])), d['marginList'], color='g')
        # # pMargin.bar(range(len(self.strategy.marginList)), self.strategy.marginList, color='g')

        # # 策略中记录的保证金
        # subplotCount += 1
        # sMargin = plt.subplot(subplotNum, 1, subplotCount)
        # sMargin.set_ylabel("sMargin")
        # # pMargin.bar(range(len(d['marginList'])), d['marginList'], color='g')
        # sMargin.bar(range(len(self.strategy.marginList)), self.strategy.marginList, color='g')

        plt.show()
