# encoding: UTF-8

"""
唐奇安通道交易策略
"""

import logging
import time
import traceback
from collections import OrderedDict
import copy

import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBase import *


class Unit(object):
    """
    Donchian Channel 的每个仓位
    """

    def __init__(self, strategy, number):
        self.number = number
        self.strategy = strategy
        # 直接使用策略的句柄
        self.log = logging.getLogger('{}.unit_{}'.format(strategy.vtSymbol, number))
        self.log.parent = self.strategy.log

        self.pos = 0
        self.vtOrders = set()
        self.vtTrades = set()
        self.tradeIDs = set()

    def empty(self):
        """
        是否空仓
        :return:
        """
        return self.pos == 0

    def saveVtOrder(self, vtOrder):
        self.vtOrders.add(vtOrder)

    def removeVtOrder(self, vtOrder):
        try:
            self.vtOrders.remove(vtOrder)
            self.log.info(u'移除停止单 {}'.format(vtOrder))
        except KeyError:
            self.log.warning(u'未找到停止单 {}'.format(vtOrder))


    def saveVtTrade(self, vtTrade):
        self.vtTrades.add(vtTrade)
        self.tradeIDs.add(vtTrade.tradeID)

    def wasDealTrade(self, tradeID):
        """

        :return:
        """
        return tradeID in self.tradeIDs


########################################################################
class DonchianChannelStrategy(CtaTemplate):
    """唐奇安通道交易策略"""
    className = u'DonchianChannelStrategy'
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
    highOut1 = None
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

    DIRECTION_LONG = 'long'
    DIRECTION_SHORT = 'short'

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

        # 仓位列表
        self.unitList = [Unit(self, i) for i in range(self.unitsNum)]  # 每一仓都是一个对象

        # vtOrderID: vtOrder
        self.vtOrders = {}

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

        # 启动后，挂停止单挂停止单
        self.sendStopOrderToOpenOnBar()

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

    def onBar(self, bar1min):
        """

        :param bar1min:a
        :return:
        """
        CtaTemplate.onBar(self, bar1min)

        #############
        bar = self.bar

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

        # 直接使用停止单
        self.sendStopOrderToOpenOnBar()

        # TODO 撤单
        # self.onBarCannelOrders()

    def onTrade(self, trade):
        """
        trade.symbol = EMPTY_STRING              # 合约代码
        trade.exchange = EMPTY_STRING            # 交易所代码
        trade.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        trade.tradeID = EMPTY_STRING             # 成交编号
        trade.vtTradeID = EMPTY_STRING           # 成交在vt系统中的唯一编号，通常是 Gateway名.成交编号

        trade.orderID = EMPTY_STRING             # 订单编号
        trade.vtOrderID = EMPTY_STRING           # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

        # 成交相关
        trade.direction = EMPTY_UNICODE          # 成交方向
        trade.offset = EMPTY_UNICODE             # 成交开平仓
        trade.price = EMPTY_FLOAT                # 成交价格
        trade.volume = EMPTY_INT                 # 成交数量, 可能会是部分成交
        trade.tradeTime = EMPTY_STRING           # 成交时间

        可能会是部分成交
        :param trade: gateWay 订单实例
        :return:
        """

        # vt订单
        vtOrder = self.getVtOrder(trade.vtOrderID)
        unit = vtOrder.unit
        # 这个订单是否已经被处理过了
        if unit.wasDealTrade(trade):
            self.log.warning(u'收到重复的 tradeID: {}'.format(trade.tradeID))
            return
        self.log.info(u'{} 成交 vtOrderID:{} tradeID:{}'.format(trade.direction, vtOrder.stopOrderID, trade.tradeID))

        # 多头开仓成交后，撤消空头委托
        if vtOrder.direction == CTAORDER_BUY:
            for stopOrder in self.getAllVtOrders():
                # 找出开空的
                if stopOrder.direction == CTAORDER_SHORT:
                    # 撤单
                    self.log.info(u'撤销 vtOrderID:{}'.format(stopOrder.stopOrderID))
                    self.cancelOrder(stopOrder.stopOrderID)

                    # unit 移除这个单
                    stopOrder.unit.removeVtOrder(stopOrder)

        # 反之同样
        if vtOrder.direction == CTAORDER_SHORT:
            for stopOrder in self.getAllVtOrders():
                # 找出开空的
                if stopOrder.direction == CTAORDER_BUY:
                    # 撤单
                    self.log.info(u'撤销 vtOrderID:{}'.format(stopOrder.stopOrderID))
                    self.cancelOrder(stopOrder.stopOrderID)

                    # unit 移除这个单
                    stopOrder.unit.removeVtOrder(stopOrder)

        # 保存已经处理过的 trade 对象
        unit.saveVtTradeID(trade)

        # 发出状态更新事件
        self.putEvent()

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
        self.highOut11 = lows[-1]
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

    def sendStopOrderToOpenOnBar(self):
        """
        挂停止单
        :return:
        """

        unit = self.getFirstUnit()
        if not unit.empty():
            # 已经有持仓了，不下开仓单
            return

        # 空仓, 可以下开仓停止单

        # 多头停止单
        self.log.info(u'挂多头停止单')
        hihgInPrices = [self.highIn1 + i * self.atr for i in range(self.unitsNum)]
        for i, price in enumerate(hihgInPrices):
            unit = self.unitList[i]
            # 连续下 unitsNum 个停止单
            vtOrderID = self.sendOrder(CTAORDER_BUY, price, self.hands, stop=True)
            # 保存订单号
            vtOrder = self.getVtOrder(vtOrderID)
            vtOrder.unit = unit
            unit.saveVtOrder(vtOrder)
            fomatter = {
                'unit': i + 1,
                'vtOrderID': vtOrderID,
                'price': price
            }
            msg = u' '.join([u'{}:{}'.format(k, v) for k, v in fomatter.items()])
            self.log.info(msg)

        # 空头停止单
        self.log.info(u'挂空头停止单')
        lowInPrices = [self.lowIn1 - i * self.atr for i in range(self.unitsNum)]
        for i, price in enumerate(lowInPrices):
            unit = self.unitList[i]
            # 连续下 unitsNum 个停止单
            vtOrderID = self.sendOrder(CTAORDER_BUY, price, self.hands, stop=True)
            # 保存订单号
            vtOrder = self.getVtOrder(vtOrderID)
            vtOrder.unit = unit
            unit.saveVtOrder(vtOrder)
            fomatter = {
                'unit': i + 1,
                'vtOrderID': vtOrderID,
                'price': price
            }
            msg = u' '.join([u'{}:{}'.format(k, v) for k, v in fomatter.items()])
            self.log.info(msg)

    def getFirstUnit(self):
        """

        :param direction: DIRECTION_LONG OR DIRECTION_SHORT
        :return:
        """
        return self.unitList[0]

    def saveVtOrder(self, vtOrder):
        self.vtOrders[vtOrder.stopOrderID] = vtOrder

    def onStopOrder(self, so):
        """
        收到停止单推送
        :param so:
        :return:
        """

        vtOrderID = so.stopOrderID
        self.log.info(u'停止单 {} '.format(so))

        if so.status == STOPORDER_WAITING:
            # 保存停止单
            self.saveVtOrder(so)
            # u'等待中' 下单成功 绑定对应的unit
        elif so.status == STOPORDER_CANCELLED:
            # u'已撤销' 剔除该单号
            self.removeVtOrderID(vtOrderID)
            so.unit.removeVtOrder(so)
        elif so.status == STOPORDER_TRIGGERED:
            # u'已触发' 剔除该单号
            self.removeVtOrderID(vtOrderID)
            so.unit.removeVtOrder(so)
        else:  # 未知状态
            self.trading = False
            self.log.error(u'vtOrder 未知的停止单状态 {}'.format(so.status))
            for vtOrderID in self.getAllVtOrderIDs():
                self.cancelOrder(vtOrderID)

    def getAllVtOrderIDs(self):
        return list(self.vtOrders.keys())

    def getAllVtOrders(self):
        return list(self.vtOrders.values())

    def removeVtOrderID(self, vtOrderID):
        try:
            self.vtOrders.pop(vtOrderID)
            self.log.info(u'移除 vtOrderID:{}'.format(vtOrderID))
        except KeyError:
            self.log.info(u'未找到可移除的 vtOrderID:{}'.format(vtOrderID))

    def getVtOrder(self, vtOrderID):
        return self.vtOrders.get(vtOrderID)
        # def setStatus(self, status):
        #     """
        #     STATUS_LONG
        #     :param status:
        #     :return:
        #     """
        #     self.status = status
        #
        # def onBarCannelOrders(self):
        #     """
        #     撤单
        #     :return:
        #     """
        #
        # def onBarChangePos(self):
        #     """
        #     根据指标计算下单，目前每分钟调仓一次
        #     :return:
        #     """
        #     # 平仓
        #     if self.status != self.STATUS_EMPTY:
        #         self.changePos2Empty()
        #
        #     # 开仓加仓
        #     if self.status == self.STATUS_EMPTY:
        #         self.changePosWhileEmpty()
        #
        # def changePos2Empty(self):
        #     """
        #     平仓
        #     :return:
        #     """
        #     if self.status in (self.STATUS_LONG, self.STATUS_LONG_FULL):
        #         # 已经开多, 判断是否平多
        #         if self.bar1min.close <= self.lowOut1:
        #             self.setTargetPos(self.pos)
        #
        #
        # def changePosWhileEmpty(self):
        #     # 多头开仓价格
        #     targetLongPrices = [self.highIn1 + num * self.atr for num in range(self.unitsNum)]
        #     posNum = 0
        #     for price in targetLongPrices:
        #         # 该 bar 只要曾经达到过最高点，就开仓
        #         if self.bar1min.high >= price:
        #             posNum += self.hands
        #             self.units += 1
        #
        #     if posNum != 0:
        #         # 下单开仓
        #         self.setTargetPos(posNum)
        #         self.setStatus(self.STATUS_LONG)
        #         lastLongPrice = targetLongPrices[self.units - 1]
        #         # 平仓位置
        #         self.stop = lastLongPrice - self.stopAtr * self.atr
        #
        #         # if self.targetPos == self.pos:
        #         #     # 调仓成功
        #         # else:
        #         #     self.log.error(u'调仓失败 target: {} pos:{}'.format(self.targetPos, self.pos))
        #         #     self.stop()
        #         #     return
        #
        #     # 空头开仓价格
        #     targetShortPrices = [self.lowIn1 - num * self.atr for num in range(self.unitsNum)]
        #     # 空头开仓
        #
        # def changePosWhileLong(self):
        #     """
        #     已经
        #     :return:
        #     """
        #     pass
