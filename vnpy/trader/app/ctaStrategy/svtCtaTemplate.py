# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import logging
import copy
from collections import OrderedDict

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
        self.log = logging.getLogger(loggerName)

        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None
        self.bar1min = None  # 1min bar
        self.bar = None  # 根据 barPeriod 聚合的 bar
        self.barCount = 0

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

    def loadBar(self, barNum):
        """读取bar数据"""
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
            bar = self.bar
            self.bar = copy.copy(bar1min)
        else:
            # 还没凑齐一个完整的 bar
            self.refreshBarByBar(self.bar, bar1min)

        self.barCount += 1

    def isNewBar(self):
        return self.barCount % self.barPeriod == 0

    def stop(self):
        self.ctaEngine.stopStrategy(self)


########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    def onBar(self, bar1min):
        vtTargetPosTemplate.onBar(self, bar1min)
        CtaTemplate.onBar(self, bar1min)
