# encoding: UTF-8

'''
本文件中实现了风控引擎，用于提供一系列常用的风控功能：
1. 委托流控（单位时间内最大允许发出的委托数量）
2. 总成交限制（每日总成交数量限制）
3. 单笔委托的委托数量控制
'''

from __future__ import division
import json
import logging

from vnpy.trader.vtConstant import *
from vnpy.trader.app.riskManager.rmEngine import RmEngine as SvtRmEngine


class RmEngine(SvtRmEngine):
    def __init__(self, mainEngine, eventEngine):
        self.log = logging.getLogger('root')
        self.marginRatioWarning = EMPTY_FLOAT

        super(RmEngine, self).__init__(mainEngine, eventEngine)
        self.log.info(u'加载风控模块')

    def writeRiskLog(self, content):
        self.log.warning(u'{}'.format(content))
        super(RmEngine, self).writeRiskLog(content)

    def checkRisk(self, orderReq, gatewayName):
        # 不至于触发风控的情况，仅发出警报
        self.warningRisk(orderReq, gatewayName)
        return super(RmEngine, self).checkRisk(orderReq, gatewayName)

    def warningRisk(self, orderReq, gatewayName):
        """
        不至于触发风控的情况，仅发出警报
        :param orderReq:
        :param gatewayName:
        :return:
        """
        # 检查保证金比例
        if gatewayName in self.marginRatioDict and self.marginRatioDict[gatewayName] >= self.marginRatioWarning:
            self.log.warning(
                u'{}接口保证金占比{}，超过预警值{}'.format(
                    gatewayName,
                    self.marginRatioDict[gatewayName],
                    self.marginRatioWarning

                ))

    def loadSetting(self):
        with open(self.settingFilePath) as f:
            d = json.load(f)
            for k, v in d.items():
                setattr(self, k, v)

    def toSaveSetting(self):
        d = super(RmEngine, self).toSaveSetting()
        d.update({
            'marginRatioWarning': self.marginRatioWarning,
        })
        return d
