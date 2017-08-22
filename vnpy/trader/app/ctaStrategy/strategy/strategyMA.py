# encoding: UTF-8

"""
MA 策略
"""

from __future__ import division

import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT
from vnpy.trader.app.ctaStrategy.ctaTemplate import TargetPosTemplate


class MAStrategy(TargetPosTemplate):
    """

    """
    className = 'EmaDemoStrategy'
    author = u'lamter'

    # 策略参数
    period = 5  # 几根均线
    initDays = 10  # 初始化数据所用的天数

    # 策略变量
    bar = None  # K线对象
    barMinute = EMPTY_STRING  # K线当前的分钟
    closeList = []  # K线最高点列表
    ma = None # 移动均线的值
    hands = 0 # 仓位是多少手

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'period',
                 'hands'
                 ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'ma'
               ]

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(MAStrategy, self).__init__(ctaEngine, setting)

        self.closeList = []

    def onInit(self):
        initData = self.loadBar(self.initDays)

        for bar in initData:
            self.onBar(bar)

        self.putEvent()

    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'MA 演示仓策略启动')
        self.putEvent()

    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'MA 演示策略停止')
        self.putEvent()

    def onTick(self, tick):
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:
            if self.bar:
                self.onBar(self.bar)

            bar = self.newBar(tick)

            self.bar = bar  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute  # 更新当前的分钟

        else:  # 否则继续累加新的K线
            self.refreshBar(tick)

        super(MAStrategy, self).onTick(tick)

    def onBar(self, bar):
        super(MAStrategy, self).onBar(bar)
        self.bar = bar

        # 填入最高点
        self.closeList.append(float(self.bar.close))

        # 计算新的 MA(5)
        self.ma = talib.MA(np.array(self.closeList[-self.period:]).astype('float'), timeperiod=self.period)[-1]

        if np.isnan(self.ma):
            # 数据不全，暂时不下单
            return


        # 调仓
        if self.bar.close > self.ma:
            # 向上突破，做多
            self.setTargetPos(self.hands)
        elif self.bar.close < self.ma:
            # 均线之下，做空
            self.setTargetPos(-self.hands)
        else:
            pass

    def onTrade(self, trade):
        pass
