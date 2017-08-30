# encoding: UTF-8

"""
唐奇安通道交易策略
"""

import time
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

    atrPeriod = 14
    unitsNum = 4  # 一共4仓

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'barPeriod',
                 'atrPeriod',
                 'unitsNum',
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

    atr = None

    hands = 1  # 每仓多少手
    units = 0  # 当前有多少仓

    stop = None

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'status',

               'highIn1',
               'highOut1',
               'lowIn1',
               'lowOut1',
               'highIn2',
               'highOut2',
               'lowIn2',
               'lowOut2',

               'atr',
               'stop',
               ]

    STATUS_EMPTY = u'空仓'  # 策略状态，空仓
    STATUS_LONG = u'开多'  # 开多，未满仓
    STATUS_LONG_FULL = u'满多'  # 开多，满仓
    STATUS_SHORT = u'开空'  # 开空，未满仓
    STATUS_SHORT_FULL = u'满空'  # 开空，满仓

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DonchianChannelStrategy, self).__init__(ctaEngine, setting)

        self.maxBarNum = max(self.in1, self.out1, self.in2, self.out2)  # 最大的入场周期
        self.barList = []
        self.highList = []  # 最高价队列
        self.lowList = []  # 最低价队列
        self.closeList = []  # 收盘价队列

        self.status = self.STATUS_EMPTY

    def onInit(self):
        initData = self.loadBar(self.maxBarNum)
        self.log.info(u'即将加载 {} 个 bar'.format(len(initData)))
        initData.sort(key=lambda bar: bar.datetime)

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

        for bar in initData:
            self.onBar(bar)

        # 计算出入场价格
        self._calIndexValue()

        if __debug__:
            self.log.info(self.varList2Log())

        if len(initData) >= self.maxBarNum:
            self.log.info(u'初始化完成')
        else:
            self.log.warning(u'初始化数据不足，初始化失败!')
            self.inited = False

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
            if self.bar1min:
                self.onBar(self.bar1min)

            self.bar1min = self.newBar(tick)
            self.barMinute = tickMinute  # 更新当前的分钟

        else:  # 否则继续累加新的K线
            self.refreshBarByTick(self.bar1min, tick)

            # 开仓之后，要实时更新出入场位置
            if self.status == self.STATUS_LONG:
                self.highIn1 = max(self.highIn1, tick.lastPrice)
                self.highIn2 = max(self.highIn2, tick.lastPrice)

            elif self.status == self.STATUS_SHORT:
                self.lowIn1 = min(self.lowIn1, tick.lastPrice)
                self.lowIn2 = min(self.lowIn2, tick.lastPrice)

        super(DonchianChannelStrategy, self).onTick(tick)

    def onBar(self, bar1min):
        """

        :param bar1min:a
        :return:
        """
        super(DonchianChannelStrategy, self).onBar(bar1min)

        #############
        bar = self.bar

        # 每分钟尝试调仓一次
        if self.trading:
            self.onBarChangePos()

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

        self.closeList.append(bar.close)
        self.closeList = self.closeList[-self.maxBarNum:]

        # 计算指标数值
        self._calIndexValue()

        if not self.trading:
            # 非交易时间段
            return

        if __debug__:
            self.log.info(self.varList2Log())

            # TODO 撤单
            # self.onBarCannelOrders()

    def onTrade(self, trade):
        pass

    def _calIndexValue(self):
        """
        计算出入场的价格
        :return:
        """
        highArray = np.array(self.highList)
        lowArray = np.array(self.lowList)
        closeArray = np.array(self.closeList)

        # 高点入场
        highs = talib.MAX(highArray, self.in1)
        self.highIn1 = highs[-1]
        highs = talib.MAX(highArray, self.in2)
        self.highIn2 = highs[-1]

        # 高点离场
        lows = talib.MIN(lowArray, self.out1)
        self.highOut1 = lows[-1]
        lows = talib.MIN(lowArray, self.out2)
        self.highOut2 = lows[-1]

        # 低点入场
        lows = talib.MIN(lowArray, self.in1)
        self.lowIn1 = lows[-1]
        lows = talib.MIN(lowArray, self.in2)
        self.lowIn2 = lows[-1]

        # 低点离场
        highs = talib.MAX(highArray, self.out1)
        self.lowOut1 = highs[-1]
        highs = talib.MAX(highArray, self.out2)
        self.lowOut2 = highs[-1]

        # 计算 atr
        atrs = talib.ATR(highArray, lowArray, closeArray, self.atrPeriod)
        self.atr = atrs[-1]

    def varList2Log(self):
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
                          'lowOut2',
                          'atr',
                          ]:
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

        return msg

    def setStatus(self, status):
        """
        STATUS_LONG
        :param status:
        :return:
        """
        self.status = status

    def onBarCannelOrders(self):
        """
        撤单
        :return:
        """

    def onBarChangePos(self):
        """
        根据指标计算下单，目前每分钟调仓一次
        :return:
        """
        # 平仓
        if self.status != self.STATUS_EMPTY:
            self.changePos2Empty()

        # 开仓加仓
        if self.status == self.STATUS_EMPTY:
            self.changePosWhileEmpty()

    def changePos2Empty(self):
        """
        平仓
        :return:
        """
        if self.status in (self.STATUS_LONG, self.STATUS_LONG_FULL):
            # 已经开多, 判断是否平多
            if self.bar1min.close <= self.lowOut1:
                self.setTargetPos(self.pos)


    def changePosWhileEmpty(self):
        # 多头开仓价格
        targetLongPrices = [self.highIn1 + num * self.atr for num in range(self.unitsNum)]
        posNum = 0
        for price in targetLongPrices:
            # 该 bar 只要曾经达到过最高点，就开仓
            if self.bar1min.high >= price:
                posNum += self.hands
                self.units += 1

        if posNum != 0:
            # 下单开仓
            self.setTargetPos(posNum)
            self.setStatus(self.STATUS_LONG)
            lastLongPrice = targetLongPrices[self.units - 1]
            # 平仓位置
            self.stop = lastLongPrice - self.stopAtr * self.atr

            # if self.targetPos == self.pos:
            #     # 调仓成功
            # else:
            #     self.log.error(u'调仓失败 target: {} pos:{}'.format(self.targetPos, self.pos))
            #     self.stop()
            #     return

        # 空头开仓价格
        targetShortPrices = [self.lowIn1 - num * self.atr for num in range(self.unitsNum)]
        # 空头开仓

    def changePosWhileLong(self):
        """
        已经
        :return:
        """
        pass
