# encoding: UTF-8

"""
唐奇安通道交易策略
"""

from datetime import time

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.svnCtaTemplate import TargetPosTemplate


########################################################################
class DonchianChannelStrategy(TargetPosTemplate):
    """唐奇安通道交易策略"""
    className = 'DonchianChannelStrategy'
    author = u'lamter'

    # 策略参数
    highIn1 = 20  # 小周期入场
    highOut1 = 10  # 小周期离场
    highIn2 = 55  # 大周期入场
    highOut2 = 20  # 大周期离场
    stopAtr = 2  # 2ATR 止损
    barPeriod = 10  # 一根bar是什么周期的

    initDays = 10

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

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               ]

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DonchianChannelStrategy, self).__init__(ctaEngine, setting)

    def onInit(self):
        initData = self.loadBar(self.initDays)

        for bar in initData:
            self.onBar(bar)

        self.putEvent()

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
            if self.bar:
                self.onBar(self.bar)

            bar = self.newBar(tick)

            self.bar = bar  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute  # 更新当前的分钟

        else:  # 否则继续累加新的K线
            self.refreshBar(self.bar, tick)

        super(DonchianChannelStrategy, self).onTick(tick)

    def onBar(self, bar):
        super(DonchianChannelStrategy, self).onBar(bar)
        self.bar = bar


    def onTrade(self, trade):
        pass
