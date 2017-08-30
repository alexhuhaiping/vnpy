# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import logging

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
        self.log = logging.getLogger('ctabacktesting')

        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None

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

    def refreshBar(self, bar, tick):
        bar.high = max(bar.high, tick.lastPrice)
        bar.low = min(bar.low, tick.lastPrice)
        bar.close = tick.lastPrice

    def paramList2Html(self):
        return {
            k: getattr(self, k) for k in self.paramList
            }

    def varList2Html(self):
        return {
            k: getattr(self, k) for k in self.varList
            }

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

########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    pass
