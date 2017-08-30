# encoding: UTF-8

"""
唐奇安通道交易策略
"""

from datetime import time
import traceback
from collections import OrderedDict
import copy

import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import TargetPosTemplate


########################################################################
class DonchianChannelStrategy(TargetPosTemplate):
    """唐奇安通道交易策略"""
    className = 'DonchianChannelStrategy'
    author = u'lamter'

    # 策略参数
    in1 = 20  # 小周期入场
    out1 = 10  # 小周期离场
    in2 = 55  # 大周期入场
    out2 = 20  # 大周期离场
    stopAtr = 2  # 2ATR 止损
    barPeriod = 10  # 一根bar是什么周期的

    # 策略变量
    bar = None  # K线对象
    barMinute = EMPTY_STRING  # K线当前的分钟
    barList = []  # K线对象的列表

    orderList = []  # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'barPeriod',
                 ]

    # 入场价格
    highIn1 = None  # 高点出入场
    higOut1 = None
    highIn2 = None
    highOut2 = None

    lowIn1 = None  # 低点出入场
    lowOut1 = None
    lowIn2 = None
    lowOut2 = None

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',

               'highIn1',
               'highOut1',
               'lowIn1',
               'lowOut1',
               'highIn2',
               'highOut2',
               'lowIn2',
               'lowOut2',
               ]

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DonchianChannelStrategy, self).__init__(ctaEngine, setting)

        self.maxBarNum = max(self.in1, self.out1, self.in2, self.out2)  # 最大的入场周期
        self.barList = []
        self.highList = []  # 最高价队列
        self.lowList = []  # 最低价队列

    def onInit(self):
        initData = self.loadBar(self.maxBarNum)
        self.log.info(u'即将加载 {} 个 bar'.format(len(initData)))
        initData.sort(key=lambda bar: bar.datetime)

        for bar in initData:
            self.onBar(bar)

        # 计算出入场价格
        self._calInOut()

        self.logVarList()

        self.putEvent()
        self.log.info(u'初始化完成')

    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.log.info(u'策略 {} 启动'.format(self.className))
        self.putEvent()

    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.log.info(u'策略 {} 停止'.format(self.className))
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

        super(DonchianChannelStrategy, self).onTick(tick)

    def onBar(self, bar1min):
        super(DonchianChannelStrategy, self).onBar(bar1min)

        #############
        bar = self.bar

        if not self.isNewBar():
            # 没有凑满新的 bar
            return

        # 保存极值队列
        self.barList.append(bar)
        self.barList = self.barList[-self.maxBarNum:]

        self.highList.append(bar.high)
        self.highList = self.highList[-self.maxBarNum:]

        self.lowList.append(bar.low)
        self.lowList = self.lowList[-self.maxBarNum:]

        # 计算出入场的价格
        self._calInOut()

        if not self.trading:
            # 非交易时间段
            return

        self.logVarList()

        # TODO 撤单
        # TODO 下单

    def onTrade(self, trade):
        pass

    def _calInOut(self):
        """
        计算出入场的价格
        :return:
        """

        # 高点入场
        highs = talib.MAX(np.array(self.highList), self.in1)
        self.highIn1 = highs[-1]
        highs = talib.MAX(np.array(self.highList), self.in2)
        self.highIn2 = highs[-1]

        # 高点离场
        lows = talib.MIN(np.array(self.lowList), self.out1)
        self.highOut1 = lows[-1]
        lows = talib.MIN(np.array(self.lowList), self.out2)
        self.highOut2 = lows[-1]

        # 低点入场
        lows = talib.MIN(np.array(self.lowList), self.in1)
        self.lowIn1 = lows[-1]
        lows = talib.MIN(np.array(self.lowList), self.in2)
        self.lowIn2 = lows[-1]

        # 低点离场
        highs = talib.MAX(np.array(self.highList), self.out1)
        self.lowOut1 = highs[-1]
        highs = talib.MAX(np.array(self.highList), self.out2)
        self.lowOut2 = highs[-1]

        if __debug__:
            print(self.bar.datetime)
            self.logVarList()

    def logVarList(self):
        if self.isBackTesting():
            # 回测中，不输出这个日志
            return
        dic = OrderedDict()

        for k in self.varList:
            dic[k] = getattr(self, k)

        if self.priceTick == int(self.priceTick):
            # 整数化
            try:
                for k in ['highIn1',
                          'highOut1',
                          'lowIn1',
                          'lowOut1',
                          'highIn2',
                          'highOut2',
                          'lowIn2',
                          'lowOut2', ]:
                    try:
                        dic[k] = int(dic[k])
                    except ValueError as e:
                        if e.message == 'cannot convert float NaN to integer':
                            pass
                        else:
                            raise

            except TypeError as e:
                if e.message == "int() argument must be a string or a number, not 'NoneType'":
                    pass
                else:
                    raise

        msg = u''
        for k, v in dic.items():
            msg += u'{}:{} '.format(k, v)

        self.log.info(msg)
