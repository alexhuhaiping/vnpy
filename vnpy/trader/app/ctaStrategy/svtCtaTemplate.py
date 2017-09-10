# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import logging
import copy
from collections import OrderedDict
import pymongo

import arrow

from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate as vtCtaTemplate
from vnpy.trader.app.ctaStrategy.ctaTemplate import TargetPosTemplate as vtTargetPosTemplate
from vnpy.trader.vtObject import VtBarData

if __debug__:
    from vnpy.trader.svtEngine import MainEngine
    from vnpy.trader.vtObject import VtContractData


########################################################################
class CtaTemplate(vtCtaTemplate):
    """CTA策略模板"""

    barPeriod = 1  # n 分钟的K线

    paramList = vtCtaTemplate.paramList[:]
    paramList.extend([
        'barPeriod'
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

        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None
        self.bar1min = None  # 1min bar
        self.bar = None  # 根据 barPeriod 聚合的 bar
        self.bar1minCount = 0

    @property
    def calssName(self):
        return self.__class__.__name__

    def onTrade(self, trade):
        """
        :param trade: VtTradeData
        :return:
        """
        raise NotImplementedError

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
        return OrderedDict(
            (k, getattr(self, k)) for k in self.paramList
        )

    def varList2Html(self):
        return OrderedDict(
            (k, getattr(self, k)) for k in self.varList
        )

    def toHtml(self):
        items = (
            ('param', self.paramList2Html()),
            ('var', self.varList2Html()),
        )
        return OrderedDict(items)

    def loadBar(self, barNum):
        """加载用于初始化策略的数据"""
        return self.ctaEngine.loadBar(self.vtSymbol, self.barCollection, barNum, self.barPeriod)

    @property
    def mainEngine(self):
        assert isinstance(self.ctaEngine.mainEngine, MainEngine)
        return self.ctaEngine.mainEngine

    @property
    def contract(self):
        """
        合约
        :return:
        """
        if self.isBackTesting():
            return '111'
        else:

            contract = self.mainEngine.getContract(self.vtSymbol)
            isinstance(contract, VtContractData)
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

        return self._priceTick

    def onBar(self, bar1min):
        if self.isBackTesting():
            self.bar1min = bar1min

        if self.bar is None:
            # 还没有任何数据
            self.bar = copy.copy(bar1min)
        elif self.isNewBar():
            # bar1min 已经凑齐了一个完整的 bar
            self.bar = copy.copy(bar1min)
        else:
            # 还没凑齐一个完整的 bar
            self.refreshBarByBar(self.bar, bar1min)

        self.bar1minCount += 1

    def isNewBar(self):
        return self.bar1minCount % self.barPeriod == 0

    def stop(self):
        self.ctaEngine.stopStrategy(self)

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        # 必要的三个字段
        dic = {
            'symbol': self.vtSymbol,
            'datetime': arrow.now().datetime,
            'class': self.className,
        }
        return dic

    def saveDB(self):
        """
        将策略的数据保存到 mongodb 数据库
        :return:
        """
        # 暂时不使用存库功能
        return
        if self.isBackTesting():
            # 回测中，不存库
            return

        # 保存
        self.ctaEngine.saveCtaDB(self.toSave())

    def fromDB(self):
        """

        :return:
        """
        filter = {
            'symbol': self.vtSymbol,
            'class': self.className,
        }
        # 对 datetime 倒叙，获取第一条
        return self.ctaEngine.ctaCol.find_one(filter, sort=[('datetime', pymongo.DESCENDING)])

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

########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    def onBar(self, bar1min):
        vtTargetPosTemplate.onBar(self, bar1min)
        CtaTemplate.onBar(self, bar1min)
