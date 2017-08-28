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


########################################################################
class CtaTemplate(vtCtaTemplate):
    """CTA策略模板"""

    def __init__(self, ctaEngine, setting):
        super(CtaTemplate, self).__init__(ctaEngine, setting)
        self.log = logging.getLogger('ctabacktesting')

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


########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    pass

