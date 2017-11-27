# encoding: UTF-8

'''
本文件中实现了风控引擎，用于提供一系列常用的风控功能：
1. 委托流控（单位时间内最大允许发出的委托数量）
2. 总成交限制（每日总成交数量限制）
3. 单笔委托的委托数量控制
'''

from __future__ import division
import logging

from vnpy.trader.app.riskManager.rmEngine import RmEngine as SvtRmEngine


class RmEngine(SvtRmEngine):
    def __init__(self, mainEngine, eventEngine):
        self.log = logging.getLogger('root')
        super(RmEngine, self).__init__(mainEngine, eventEngine)

    def writeRiskLog(self, content):
        super(RmEngine, self).writeRiskLog(content)

