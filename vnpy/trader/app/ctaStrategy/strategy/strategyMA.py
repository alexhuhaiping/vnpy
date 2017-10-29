# encoding: UTF-8

"""
MA 策略
"""

from __future__ import division

import time

import talib
import numpy as np

from vnpy.trader.vtObject import *
from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate


class MAStrategy(CtaTemplate):
    """

    """
    className = 'MAStrategy'
    author = u'lamter'
    # 策略参数
    period = 5  # 几根均线
    initDays = 10  # 初始化数据所用的天数

    # 策略变量
    bar = None  # K线对象
    barPeriod = 15  # K线当前的分钟
    closeList = []  # K线收盘列表

    ma = None  # 移动均线的值
    hands = 1  # 仓位是多少手

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'hands',
    ])

    # 变量列表，保存了变量的名称
    varList = CtaTemplate.varList[:]
    varList.extend([
        'ma',
    ])

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(MAStrategy, self).__init__(ctaEngine, setting)

        self.closeList = []

    def onInit(self):
        initData = self.loadBar(self.period)

        for bar in initData:
            self.onBar(bar)

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

        self.putEvent()

    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'MA 演示仓策略启动')
        if __debug__:
            self.log.debug(u'测试下单')
            self.sendOrder(CTAORDER_BUY, 3900, self.hands, stop=True)
        self.putEvent()

    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'MA 演示策略停止')
        self.putEvent()

    def onTick(self, tick):
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:
            if self.bar1min:
                self.onBar(self.bar1min)

            self.bar1min = self.newBar(tick)
            self.barMinute = tickMinute  # 更新当前的分钟

        else:  # 否则继续累加新的K线
            self.refreshBarByTick(self.bar1min, tick)

    def onBar(self, bar1min):
        CtaTemplate.onBar(self, bar1min)

        if not self.isNewBar():
            # 尚未累积到一个 new bar
            return

        #############
        bar = self.bar

        assert isinstance(bar1min, VtBarData)
        assert isinstance(self.bar, VtBarData)
        # 填入最高点
        self.closeList.append(float(self.bar.close))

        # 计算新的 MA(5)
        self.ma = talib.MA(np.array(self.closeList[-self.period:]).astype('float'), timeperiod=self.period)[-1]

        if np.isnan(self.ma):
            # 数据不全，暂时不下单
            return

        if not self.trading:
            return

        # 调仓
        if self.bar.close > self.ma:
            # 向上突破，做多
            if self.pos < 0:
                # 有空仓，先平仓
                self.log.info(u'反手 平空')
                stopOrderID = self.sendOrder(CTAORDER_COVER, self.bar1min.high, self.pos, stop=True)
                # 在开仓
                self.log.info(u'开多')
                self.sendOrder(CTAORDER_BUY, self.bar1min.low, self.hands, stop=True)
            else:
                addHands = self.pos - self.hands
                if addHands > 0:
                    self.sendOrder(CTAORDER_BUY, self.bar1min.low, addHands, stop=True)

        elif self.bar.close < self.ma:
            # 均线之下，做空
            if self.pos > 0:
                # 有空仓，先平仓
                self.log.info(u'反手 平多')
                self.sendOrder(CTAORDER_SELL, self.bar1min.low, -self.pos, stop=True)
                # 在开仓
                self.log.info(u'开空')
                self.sendOrder(CTAORDER_SHORT, self.bar1min.high, self.hands, stop=True)
            else:
                addHands = abs(self.pos) - self.hands
                if addHands > 0:
                    self.sendOrder(CTAORDER_SHORT, self.bar1min.high, addHands, stop=True)
        else:
            pass

    def onTrade(self, trade):
        self.log.debug(u'=======================')
        for k, v in trade.__dict__.items():
            self.log.debug(u'{}\t{}'.format(k, v))

    def onOrder(self, order):
        self.log.debug(u'=======================')
        for k, v in order.__dict__.items():
            self.log.debug(u'{}\t{}'.format(k, v))

    def onStopOrder(self, so):
        self.log.debug(u'=======================')
        for k, v in so.__dict__.items():
            self.log.debug(u'{}\t{}'.format(k, v))
