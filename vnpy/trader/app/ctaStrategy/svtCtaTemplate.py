# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import time
import talib
import logging
import copy
from collections import OrderedDict
import pymongo
from itertools import chain
import datetime

import pandas as pd
import arrow

from vnpy.trader.vtConstant import *
from vnpy.trader.vtEvent import *
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate as vtCtaTemplate
from vnpy.trader.app.ctaStrategy.ctaTemplate import ArrayManager as VtArrayManager
from vnpy.trader.app.ctaStrategy.ctaTemplate import BarManager as VtBarManager
from vnpy.trader.app.ctaStrategy.ctaTemplate import TargetPosTemplate as vtTargetPosTemplate
from vnpy.trader.vtObject import VtBarData, VtCommissionRate

if __debug__:
    from vnpy.trader.svtEngine import MainEngine
    from vnpy.trader.vtEngine import DataEngine
    from vnpy.trader.vtObject import VtContractData


########################################################################
class CtaTemplate(vtCtaTemplate):
    """CTA策略模板"""

    barXmin = 1  # n 分钟的K线

    # 默认初始资金是1万, 在 onTrade 中平仓时计算其盈亏
    # 有存库的时候使用存库中的 capital 值，否则使用 CTA_setting.json 中的值
    capital = 10000

    paramList = vtCtaTemplate.paramList[:]
    paramList.extend([
        'barXmin',
        'marginRate',
        'capital',
    ])
    varList = vtCtaTemplate.varList[:]
    varList.extend([

    ])

    def __init__(self, ctaEngine, setting):
        super(CtaTemplate, self).__init__(ctaEngine, setting)
        loggerName = 'ctabacktesting' if self.isBackTesting() else 'cta'
        logger = logging.getLogger(loggerName)
        # 定制 logger.name
        self.log = logging.getLogger(self.vtSymbol)
        # self.log.parent = logger
        self.log.propagate = 0

        for f in logger.filters:
            self.log.addFilter(f)
        for h in logger.handlers:
            self.log.addHandler(h)

        if self.isBackTesting():
            self.log.setLevel(logger.level)

        # 复制成和原来的 Logger 配置一样

        if not isinstance(self.barXmin, int):
            raise ValueError(u'barXmin should be int.')

        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None
        self._size = None  # 每手的单位
        # self.bar1min = None  # 1min bar
        # self.bar = None  # 根据 barXmin 聚合的 bar
        # self.bar1minCount = 0

        self._pos = 0
        self.posList = []
        self._marginRate = None
        self.commissionRate = None  # 手续费率 vtObject.VtCommissionRate
        self.marginList = []
        self._positionDetail = None  # 仓位详情

        # K线管理器
        self.maxBarNum = 0
        self.initMaxBarNum()
        self.bm = BarManager(self, self.onBar, self.barXmin, self.onXminBar)  # 创建K线合成器对象
        # 技术指标生成器
        self.am = ArrayManager(self.maxBarNum)

        self.registerEvent()

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, pos):
        self._pos = pos

        if self.inited and self.trading and self.isBackTesting():
            self.posList.append(pos)
            try:
                margin = self._pos * self.size * self.bar.close * self.marginRate
                self.marginList.append(abs(margin / self.capital))
            except AttributeError as e:
                if self.bar is None:
                    pass
            except TypeError:
                if self.marginRate is None:
                    pass

    @property
    def bar(self):
        return self.bm.bar

    @property
    def xminBar(self):
        return self.bm.xminBar

    @property
    def calssName(self):
        return self.__class__.__name__

    def onTrade(self, trade):
        """
        :param trade: VtTradeData
        :return:
        """
        raise NotImplementedError

    def getCharge(self, offset, price):
        # 手续费
        if self.isBackTesting():
            # 回测时使用的是比例手续费
            return self.ctaEngine.rate * self.size * price
        else:
            # TODO 实盘手续费
            return 0

    def charge(self, offset, price, volume):
        """
        扣除手续费
        :return:
        """
        charge = volume * self.getCommission(price, volume, offset)
        self.log.info(u'手续费 {}'.format(charge))
        self.capital -= charge

    def chargeSplipage(self, volume):
        """
        回测时的滑点
        :param volume:
        :return:
        """
        if self.isBackTesting():
            slippage = volume * self.size * self.ctaEngine.slippage
            self.capital -= slippage

    def newBar(self, tick):
        bar = VtBarData()
        bar.vtSymbol = tick.vtSymbol
        bar.symbol = tick.symbol
        bar.exchange = tick.exchange

        bar.open = tick.lastPrice
        bar.high = tick.lastPrice
        bar.low = tick.lastPrice
        bar.close = tick.lastPrice

        bar.date = tick.date
        bar.time = tick.time
        bar.datetime = tick.datetime  # K线的时间设为第一个Tick的时间

        # 实盘中用不到的数据可以选择不算，从而加快速度
        bar.volume = tick.volume
        bar.openInterest = tick.openInterest
        return bar

    def refreshBarByTick(self, bar, tick):
        bar.high = max(bar.high, tick.lastPrice)
        bar.low = min(bar.low, tick.lastPrice)
        bar.close = tick.lastPrice

    def refreshBarByBar(self, bar, bar1min):
        bar.high = max(bar.high, bar1min.high)
        bar.low = min(bar.low, bar1min.low)
        bar.close = bar1min.close

    def paramList2Html(self):
        orDic = OrderedDict()
        for k in self.paramList:
            orDic[k] = getattr(self, k)
            self.log.info(u'2html {} {}'.format(k, orDic[k]))
        return orDic

    def varList2Html(self):
        orDic = OrderedDict()
        for k in self.varList:
            orDic[k] = getattr(self, k)
            self.log.info(u'2html {} {}'.format(k, orDic[k]))
        return orDic

    def toHtml(self):
        self.log.info(u'生成网页')
        items = (
            ('param', self.paramList2Html()),
            ('var', self.varList2Html()),
        )
        self.log.info(u'1111111111')
        orderDic = OrderedDict(items)
        orderDic['bar'] = self.barToHtml()
        orderDic['{}minBar'.format(self.barXmin)] = self.xminBarToHtml()
        self.log.info(u'2222222222')

        # 本地停止单
        stopOrders = self.ctaEngine.getAllStopOrdersSorted(self.bm.lastTick)
        units = [so.toHtml() for so in stopOrders]
        orderDic['stopOrder'] = pd.DataFrame(units).to_html()
        self.log.info(u'333333333333')

        # 持仓详情
        orderDic['posdetail'] = self.positionDetail.toHtml()
        self.log.info(u'4444444444')

        return orderDic

    @property
    def positionDetail(self):
        if self._positionDetail is None:
            self._positionDetail = self.dataEngine.getPositionDetail(self.vtSymbol)
        return self._positionDetail

    def loadBar(self, barNum):
        """加载用于初始化策略的数据"""
        return self.ctaEngine.loadBar(self.vtSymbol, self.barCollection, barNum + 1, self.barXmin)

    @property
    def mainEngine(self):
        assert isinstance(self.ctaEngine.mainEngine, MainEngine)
        return self.ctaEngine.mainEngine

    @property
    def dataEngine(self):
        assert isinstance(self.ctaEngine.mainEngine.dataEngine, DataEngine)
        return self.ctaEngine.mainEngine.dataEngine

    @property
    def contract(self):
        """
        合约
        :return:
        """
        if self.isBackTesting():
            return self.ctaEngine.vtContract
        else:
            contract = self.mainEngine.getContract(self.vtSymbol)
            assert isinstance(contract, VtContractData) or contract is None
            return contract

    def isBackTesting(self):
        return self.getEngineType() == ENGINETYPE_BACKTESTING

    @property
    def priceTick(self):
        if self._priceTick is None:
            if self.isBackTesting():
                # 回测中
                self._priceTick = self.ctaEngine.priceTick
            else:
                # 实盘
                self._priceTick = self.contract.priceTick

        assert isinstance(self._priceTick, float) or isinstance(self._priceTick, int)
        return self._priceTick

    @property
    def marginRate(self):
        try:
            return self._marginRate.marginRate
        except AttributeError:
            if self.isBackTesting():
                # 回测中
                self._marginRate = self.ctaEngine.marginRate
                return self.marginRate
            else:
                return 0.9

    def getCommission(self, price, volume, offset):
        """

        :param price:
        :param volume:
        :param offset:
        :return:
        """

        if self.isBackTesting():
            # 回测中
            m = self.ctaEngine.rate
        else:

            m = self.commissionRate

        assert isinstance(m, VtCommissionRate)

        if offset == OFFSET_OPEN:
            # 开仓
            # 直接将两种手续费计费方式累加
            value = m.openRatioByMoney * price * volume
            value += m.openRatioByVolume * volume
        elif offset == OFFSET_CLOSE:
            # 平仓
            value = m.closeRatioByMoney * price * volume
            value += m.closeRatioByVolume * volume
        elif offset == OFFSET_CLOSEYESTERDAY:
            # 平昨
            value = m.closeRatioByMoney * price * volume
            value += m.closeRatioByVolume * volume
        elif offset == OFFSET_CLOSETODAY:
            # 平今
            value = m.closeTodayRatioByMoney * price * volume
            value += m.closeTodayRatioByVolume * volume
        else:
            err = u'未知的开平方向 {}'.format(offset)
            self.log.error(err)
            raise ValueError(err)
        return value

    @property
    def size(self):
        if self._size is None:
            if self.isBackTesting():
                # 回测中
                self._size = self.ctaEngine.size
            else:
                # 实盘
                self._size = self.contract.size

        assert isinstance(self._size, float) or isinstance(self._size, int)
        return self._size

    def onXminBar(self, xminBar):
        raise NotImplementedError(u'尚未定义')

        # def onBar(self, bar1min):
        # if self.isBackTesting():
        #     self.bar1min = bar1min
        #
        # if self.bar is None:
        #     # 还没有任何数据
        #     self.bar = copy.copy(bar1min)
        # elif self.isNewBar():
        #     # bar1min 已经凑齐了一个完整的 bar
        #     self.bar = copy.copy(bar1min)
        # else:
        #     # 还没凑齐一个完整的 bar
        #     self.refreshBarByBar(self.bar, bar1min)
        #
        # self.bar1minCount += 1

    # def isNewBar(self):
    #     return self.bar1minCount % self.barXmin == 0 and self.bar1minCount != 0

    def stop(self):
        # 执行停止策略
        self.ctaEngine.stopStrategy(self)

    def loadCtaDB(self, document):
        if not document:
            return
        self.capital = document['capital']

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        dic = self.filterSql()

        dic['datetime'] = arrow.now().datetime
        dic['capital'] = self.capital

        return dic

    def saveDB(self):
        """
        将策略的数据保存到 mongodb 数据库
        :return:
        """
        if self.isBackTesting():
            # 回测中，不存库
            return

        # 保存
        document = self.toSave()
        self.ctaEngine.saveCtaDB(self.filterSql(), {'$set': document})

    def filterSql(self):
        gateWay = self.mainEngine.getGateway('CTP')
        return {
            'symbol': self.vtSymbol,
            'className': self.className,
            'userID': gateWay.tdApi.userID,
        }

    def fromDB(self):
        """

        :return:
        """
        # 对 datetime 倒叙，获取第一条
        return self.ctaEngine.ctaCol.find_one(self.filterSql(), sort=[('datetime', pymongo.DESCENDING)])

    def onOrder(self, order):
        """
        order.direction
            # 方向常量
            DIRECTION_NONE = u'无方向'
            DIRECTION_LONG = u'多'
            DIRECTION_SHORT = u'空'
            DIRECTION_UNKNOWN = u'未知'
            DIRECTION_NET = u'净'
            DIRECTION_SELL = u'卖出'              # IB接口
            DIRECTION_COVEREDSHORT = u'备兑空'    # 证券期权

        order.offset
            # 开平常量
            OFFSET_NONE = u'无开平'
            OFFSET_OPEN = u'开仓'
            OFFSET_CLOSE = u'平仓'
            OFFSET_CLOSETODAY = u'平今'
            OFFSET_CLOSEYESTERDAY = u'平昨'
            OFFSET_UNKNOWN = u'未知'
        :param order:
        :return:
        """

        raise NotImplementedError

    def roundToPriceTick(self, price):
        """取整价格到合约最小价格变动"""
        if not self.priceTick:
            return price

        newPrice = round(price / self.priceTick, 0) * self.priceTick
        return newPrice

    def barToHtml(self):
        bar = self.bm.bar
        if bar is None:
            return u'bar 无数据'
        itmes = (
            ('datetime', bar.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', bar.open,),
            ('high', bar.high),
            ('low', bar.low),
            ('close', bar.close),
        )
        return OrderedDict(itmes)

    def xminBarToHtml(self):
        bar = self.bm.xminBar
        if bar is None:
            return u'xminBar 无数据'

        itmes = (
            ('datetime', bar.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', bar.open,),
            ('high', bar.high),
            ('low', bar.low),
            ('close', bar.close),
        )
        return OrderedDict(itmes)

    def registerEvent(self):
        """注册事件监听"""
        if self.isBackTesting():
            # 回测中不注册监听事件
            return
        en = self.ctaEngine.mainEngine.eventEngine
        en.register(EVENT_MARGIN_RATE, self.updateMarginRate)
        en.register(EVENT_COMMISSION_RATE, self.updateCommissionRate)

    def updateMarginRate(self, event):
        """更新合约数据"""
        marginRate = event.dict_['data']
        if marginRate.vtSymbol != self.vtSymbol:
            return

        self._marginRate = marginRate

    def updateCommissionRate(self, event):
        """更新合约数据"""
        commissionRate = event.dict_['data']

        # commissionRate.vtSymbol 可能为 'rb' 或者 'rb1801' 前者说明合约没改过，后者说明该合约有变动
        if commissionRate.underlyingSymbol == self.vtSymbol:
            # 返回 rb1801, 合约有变动，强制更新
            self.commissionRate = commissionRate
            return
        elif self.vtSymbol.startswith(commissionRate.underlyingSymbol):
            # 返回 rb ,合约没有变动
            self.commissionRate = commissionRate
            return
        else:
            pass

    def initContract(self):
        """
        初始化订阅合约
        :return:
        """
        waitContractSeconds = 0
        while self.contract is None:
            waitContractSeconds += 1
            if waitContractSeconds > 10:
                self.inited = False
                self.log.error(u'策略未能订阅合约 {}'.format(self.vtSymbol))
                return
            self.log.info(u'等待合约 {}'.format(self.vtSymbol))
            time.sleep(1)
        else:
            self.log.info(u'订阅合约 {} 成功'.format(self.vtSymbol))

    def initMaxBarNum(self):
        """
        初始化最大 bar 数
        :return:
        """
        self.maxBarNum = 0
        raise NotImplementedError(u'')


########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    def onBar(self, bar1min):
        vtTargetPosTemplate.onBar(self, bar1min)
        CtaTemplate.onBar(self, bar1min)


#########################################################################
class BarManager(VtBarManager):
    def __init__(self, strategy, onBar, xmin=0, onXminBar=None):
        super(BarManager, self).__init__(onBar, xmin, onXminBar)
        self.strategy = strategy
        # 当前已经加载了几个1min bar。当前未完成的 1minBar 不计入内
        self.count = 0

    # ----------------------------------------------------------------------
    def updateTick(self, tick):
        """TICK更新"""
        newMinute = False  # 默认不是新的一分钟
        oldBar = None

        # 剔除错误数据
        if self.lastTick and tick.datetime - self.lastTick.datetime > datetime.timedelta(seconds=60 * 10):
            # 如果当前 tick 比上一个 tick 差距达到 10分钟没成交的合约，则认为是错误数据
            # CTA 策略默认使用比较活跃的合约
            return

        # 尚未创建对象
        if not self.bar:
            self.bar = VtBarData()
            newMinute = True
        # 新的一分钟
        elif self.bar.datetime.minute != tick.datetime.minute:
            # 生成上一分钟K线的时间戳
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            # 创建新的K线对象
            oldBar, self.bar = self.bar, VtBarData()
            newMinute = True

        # 初始化新一分钟的K线数据
        if newMinute:
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
        # 累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, tick.lastPrice)
            self.bar.low = min(self.bar.low, tick.lastPrice)

        # 通用更新部分
        self.bar.close = tick.lastPrice
        self.bar.datetime = tick.datetime
        self.bar.openInterest = tick.openInterest

        if self.lastTick:
            self.bar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量

        if newMinute and oldBar:
            # 推送已经结束的上一分钟K线
            self.onBar(oldBar)

        # 缓存Tick
        self.lastTick = tick

    def updateBar(self, bar):
        """1分钟K线更新"""
        if self.strategy.isBackTesting():
            self.bar = bar

        self.count += 1

        # 尚未创建对象
        if not self.xminBar:
            self.xminBar = VtBarData()

            self.xminBar.vtSymbol = bar.vtSymbol
            self.xminBar.symbol = bar.symbol
            self.xminBar.exchange = bar.exchange

            self.xminBar.open = bar.open
            self.xminBar.high = bar.high
            self.xminBar.low = bar.low
            # 累加老K线
        else:
            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)

        # 通用部分
        self.xminBar.close = bar.close
        self.xminBar.datetime = bar.datetime
        self.xminBar.openInterest = bar.openInterest
        self.xminBar.volume += int(bar.volume)

        # X分钟已经走完
        if self.count % self.xmin == 0:  # 可以用X整除
            # 生成上一X分钟K线的时间戳
            self.xminBar.datetime = self.xminBar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.xminBar.date = self.xminBar.datetime.strftime('%Y%m%d')
            self.xminBar.time = self.xminBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onXminBar(self.xminBar)

            if self.strategy.isBackTesting():
                self.xminBar = None
            else:
                # 清空老K线缓存对象
                self.xminBar = VtBarData()
                # 直接将当前的 1min bar 数据 copy 到 xminBar
                for k, v in self.bar.__dict__.items():
                    setattr(self.xminBar, k, v)


#########################################################################
class ArrayManager(VtArrayManager):
    # ----------------------------------------------------------------------
    def ma(self, n, array=False):
        """简单均线"""
        result = talib.MA(self.close, n)
        if array:
            return result
        return result[-1]

    def __init__(self, size=100):
        size += 1
        super(ArrayManager, self).__init__(size)

    # ----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)

        if array:
            return result
        if result[-1] == 0:
            print(self.high)
            print(self.low)
            print(self.close)
        return result[-1]
