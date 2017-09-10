# encoding: UTF-8

"""
唐奇安通道交易策略
"""

import logging
import time
import traceback
from collections import OrderedDict
import copy
import arrow
import datetime

import talib
import numpy as np
import pandas as pd

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBase import *

if __debug__:
    from vnpy.trader.vtObject import VtOrderData, VtTradeData

# 各种平仓类型
OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


class Unit(object):
    """
    Donchian Channel 的每个仓位
    """

    # 即将离场的状态
    STOP_STATUS_ATR = 'atr'
    STOP_STATUS_OUT = 'out'

    def __init__(self, strategy, number):
        self.number = number
        self.strategy = strategy
        # 直接使用策略的句柄
        self.log = logging.getLogger('{}.unit_{}'.format(strategy.vtSymbol, number))
        self.log.parent = self.strategy.log

        self.targetPos = 0  # 目标持仓
        self.pos = 0  # 几手持仓
        self.openCost = 0  # 开仓总价
        self.closeCost = 0  # 平仓总价
        self.atrStop = None  # 止损价
        self.stopStatus = self.STOP_STATUS_ATR
        self.stopOrderSet = set()
        # self.vtTrades = set()
        self.tradeIDs = set()
        self.vtOrderID = None  # 由 stopOrder 触发的限价单订单ID
        self.dealVtOrderIDs = set()
        self.highIn = None
        self.lowIn = None

    @property
    def unitOpenCost(self):
        try:
            return self.openCost / self.pos
        except ZeroDivisionError:
            return None

    @property
    def unitCloseCost(self):
        try:
            return self.closeCost / self.pos
        except ZeroDivisionError:
            return None

    def toHtml(self):
        items = (
            ('number', self.number + 1),
            ('targetPos', self.targetPos),
            ('pos', self.pos),
            ('highIn', self.highIn),
            ('lowIn', self.lowIn),
            ('openCost', self.openCost),
            ('unitOpenCost', self.unitOpenCost),
            ('closeCost', self.closeCost),
            ('unitCloseCost', self.unitCloseCost),
            ('atrStop', self.atrStop),
        )
        orderDic = OrderedDict()
        for k, v in items:
            if isinstance(v, float):
                try:
                    # 尝试截掉过长的浮点数
                    v = u'%0.3f' % v
                    while v.endswith('0'):
                        v = v[:-1]
                    if v.endswith('.'):
                        v = v[:-1]
                except:
                    pass
            orderDic[k] = v
        return orderDic

    def setTargetPos(self, targetPos):
        self.log.info(u'设置目标仓位 {}'.format(targetPos))
        self.targetPos = targetPos

    def reset(self):
        self.log.info(u'重置 {}'.format(self))
        if self.pos != 0:
            self.log.warning(u'pos != 0')

        # self.targetPos = 0  # 目标持仓
        self.setTargetPos(0)  # 目标持仓
        self.openCost = 0  # 开仓总价
        self.closeCost = 0  # 平仓总价
        self.atrStop = None  # 止损价
        # self.stopOrderSet = set() # 运行中不能清除停止单
        # self.vtTrades = set()
        self.tradeIDs = set()
        self.vtOrderID = None  # 由 stopOrder 触发的限价单订单ID
        # self.dealVtOrderIDs = set() # 不能直接清除 已处理过的vtOrderID
        self.clearDealVtOrderIDs()

        self.highIn = None
        self.lowIn = None

    def clearDealVtOrderIDs(self):
        for vtOrderID in list(self.dealVtOrderIDs):
            if arrow.now() - arrow.get(vtOrderID.split('.')[-2]).datetime >= datetime.timedelta(days=2):
                self.dealVtOrderIDs.remove(vtOrderID)

    def __str__(self):
        s = u'< Unit.{} '.format(self.number)
        s += u'targetPos:{} '.format(self.targetPos)
        s += u'pos:{} '.format(self.pos)
        s += u'vtOrderID:{} '.format(self.vtOrderID)
        return s

    def empty(self):
        """
        是否空仓
        :return:
        """
        return self.pos == 0

    def saveStopOrder(self, stopOrder):
        self.stopOrderSet.add(stopOrder)

    def removeStopOrder(self, stopOrder):
        try:
            self.stopOrderSet.remove(stopOrder)
            # self.log.info(u'策略移除停止单 {}'.format(stopOrder))
        except KeyError:
            self.log.warning(u'未找到停止单 {}'.format(stopOrder))

    def saveVtTrade(self, vtTrade):
        # self.vtTrades.add(vtTrade)
        self.tradeIDs.add(vtTrade.tradeID)

    def wasDealTrade(self, tradeID):
        """

        :return:
        """
        return tradeID in self.tradeIDs

    def wasDealVtOrderID(self, vtOrderID):
        """

        :return:
        """
        return vtOrderID in self.dealVtOrderIDs

    def calOpenCost(self, trade):
        self.openCost += trade.volume * trade.price
        self.pos += trade.volume
        self.log.info(u'开仓成本 openCost:{} pos:{}'.format(self.openCost, self.pos))
        self.log.info(u'开仓均价 unitOpenCost:{} '.format(self.unitOpenCost))

    def calCloseCost(self, trade):
        """

        :param trade:
        :return:
        """
        self.closeCost += trade.volume * trade.price
        self.pos -= trade.volume
        self.log.info(u'平仓成本 openCost:{} pos:{}'.format(self.openCost, self.pos))
        self.log.info(u'平仓均价 unitCloseCost:{}'.format(self.unitCloseCost))

    def getAllCloseStopOrder(self, direction):
        assert direction in (CTAORDER_SELL, CTAORDER_COVER)
        return [stopOrder for stopOrder in self.stopOrderSet if stopOrder.direction == direction]


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
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'atrPeriod',
        'unitsNum',
        'hands',
    ])

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

    hands = 5  # 每仓多少手
    units = 0  # 当前有多少仓

    # stop = None

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
               # 'stop',
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

        # stopOrderID: stopOrder
        self.stopOrders = {}
        self.vtOrderID2Unit = {}  # {vtOrderID : unit}

    def onInit(self):
        self.log.info(self.paramList2log())
        for u in self.unitList:
            u.clearDealVtOrderIDs()
        # 从数据库加载策略数据
        if not self.isBackTesting():
            document = self.fromDB()
            self.loadCtaDB(document)

        initData = self.loadBar(self.maxBarNum)
        self.log.info(u'即将加载 {} 个 bar'.format(len(initData)))
        initData.sort(key=lambda bar: bar.datetime)

        self.log.info(u'initData {} to {}'.format(initData[0].datetime, initData[-1].datetime))

        if __debug__:
            self.log.debug(u'最后的 bar {}'.format(initData[-1].datetime))
            # for bar in initData:
            #     # self.log.debug(u'{}'.format(str(bar.__dict__)))
            #     self.log.debug(u'{} {} '.format(bar.high, bar.low))

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

        # if __debug__:
        #     self.log.debug(u'强制入场')
        #     self.highIn1 = 4330

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

        # 更新止损价
        self.sendStopOrder2CloseOnBar()

        if not self.isNewBar():
            # 尚未累积到一个 new bar
            return

        #############
        bar = self.bar

        assert isinstance(bar1min, VtBarData)
        assert isinstance(self.bar, VtBarData)

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
        varLogPre = self.varList2Log()
        self._calIndexValue()

        if not self.trading:
            # 非交易时间段
            return

        varLogLater = self.varList2Log()
        if varLogPre != varLogLater:
            self.log.info(varLogLater)

        # 直接使用停止单
        self.sendStopOrderToOpenOnBar()

        # TODO 撤单
        # self.onBarCannelOrders()

    def onOrder(self, order):
        """

        vtOrder.status:
            STATUS_NOTTRADED = u'pending'
            STATUS_PARTTRADED = u'partial filled'
            STATUS_ALLTRADED = u'filled'
            STATUS_CANCELLED = u'cancelled'
            STATUS_REJECTED = u'rejected'
            STATUS_UNKNOWN = u'unknown'

        :param vtOrder:
        :return:
        """

        assert isinstance(order, VtOrderData)

        vtOrderID = order.vtOrderID

        self.log.info(u'vtOrder:{} status: {}'.format(vtOrderID, order.status))

        unit = self.getUnitByVtOrderID(vtOrderID)

        if unit is None:
            self.log.warning(u'订单 {} 没有对应的 unit'.format(vtOrderID))

        if unit.wasDealVtOrderID(vtOrderID):
            self.log.waring(u'unit {} 已经处理过 '.format(unit))
            return

        firstUnit = self.getFirstUnit()
        lastUnit = self.getLastUnit()

        # 订单方向和处理
        if self.isOpenLong(order):
            # 开多
            # 设置状态
            self.setOpenStatusOnOrder(unit, order, lastUnit, self.STATUS_LONG, STATUS_ALLTRADED, self.STATUS_LONG_FULL)

        elif self.isOpenShort(order):
            # 开空
            # 设置状态
            self.setOpenStatusOnOrder(unit, order, lastUnit, self.STATUS_SHORT, STATUS_ALLTRADED,
                                      self.STATUS_SHORT_FULL)

        elif self.isCloseLong(order):
            # 平多
            # 汇报盈利
            self.logProfileOnOrder(unit, order, STATUS_ALLTRADED)

        elif self.isCloseShort(order):
            # 平空
            # 汇报盈利
            self.logProfileOnOrder(unit, order, STATUS_ALLTRADED)

        else:
            err = u'vtOrderID {} 未知的交易方向 {}'.format(vtOrderID, order.direction)
            self.log.error(err)
            raise ValueError(err)

        # 完全平仓，要在汇报盈利之前
        if self.isCloseShort(order) or self.isCloseLong(order):
            if unit is firstUnit and order.status == STATUS_ALLTRADED:
                # 首仓,全部成交,无持仓
                # 重置 unit
                self.resetAllUnit()
                # 设置为空仓
                self.setStatus(self.STATUS_EMPTY)

    def onTrade(self, trade):
        """
        可能会是部分成交
        :param trade: gateWay 订单实例
        :return:
        """
        assert isinstance(trade, VtTradeData)
        unit = self.getUnitByVtOrderID(trade.vtOrderID)

        # 这个订单是否已经被处理过了
        if unit.wasDealTrade(trade):
            self.log.warning(u'收到重复的 tradeID: {}'.format(trade.tradeID))
            return

        self.log.info(u'{} 成交 vtOrderID:{} tradeID:{}'.format(trade.direction, trade.vtOrderID, trade.tradeID))

        # 保存已经处理过的 trade 对象
        unit.saveVtTrade(trade)

        if self.isOpenLong(trade):
            # 开多
            # 成交后，撤消开空委托
            self.cancelOnTrade(CTAORDER_SHORT)

            # 计算成本价
            unit.calOpenCost(trade)

            # TODO 手续费

            # 下止损单
            self.sendStopOrder2CloseOnTrade(unit, trade, CTAORDER_SELL)

        elif self.isOpenShort(trade):
            # 开空
            # 开仓成交后，撤消开多委托
            self.cancelOnTrade(CTAORDER_BUY)

            # 计算成本价
            unit.calOpenCost(trade)

            # TODO 手续费

            # 下止损单
            self.sendStopOrder2CloseOnTrade(unit, trade, CTAORDER_COVER)

        elif self.isCloseLong(trade):
            # 平多

            # 计算平仓成本价
            unit.calCloseCost(trade)

        elif self.isCloseShort(trade):
            # 平空

            # 计算平仓成本价
            unit.calCloseCost(trade)

        else:
            self.log.error(u'未知的成交 {}'.format(trade.tradeID))

        # 发出状态更新事件
        self.putEvent()

    def sendStopOrder2CloseOnTrade(self, unit, trade, closeCtaOrderDirection):
        """
        下平仓单
        :param unit:
        :param trade:
        :param direction:
        :return:
        """

        # 先撤掉平仓单
        for closeStopOrder in unit.getAllCloseStopOrder(closeCtaOrderDirection):
            # 撤单
            self.cancelOrder(closeStopOrder.stopOrderID)
            # unit.removeStopOrder(closeStopOrder)

        # 下平仓单，两个平仓的位置，一个是 2atr，一个是 10k 低点
        if closeCtaOrderDirection == CTAORDER_SELL:
            atrStopPrice = unit.unitOpenCost - self.stopAtr * self.atr  # 2atr止损价格
            outPrice = self.highOut1  # 离场
            if atrStopPrice > outPrice:
                # 2atr止损
                stopPrice = atrStopPrice
            else:
                # 周期离场
                stopPrice = outPrice
                unit.setStopStatus(Unit.STOP_STATUS_OUT)

        elif closeCtaOrderDirection == CTAORDER_COVER:
            atrStopPrice = unit.unitOpenCost + self.stopAtr * self.atr  # 2atr止损价格
            outPrice = self.lowOut1  # 离场
            if atrStopPrice < outPrice:
                # 2atr止损
                stopPrice = atrStopPrice
            else:
                # 周期离场
                stopPrice = outPrice
                unit.setStopStatus(Unit.STOP_STATUS_OUT)
        else:
            msg = u'无法下止损单，未知的平仓方向 {}'.format(closeCtaOrderDirection)
            self.log.error(msg)
            raise ValueError(msg)

        # 如果是部分成交，那么也部分下单平仓
        # 2atr止损单
        self.log.info(u'止损单')
        vtoid = self.sendOrder(closeCtaOrderDirection, stopPrice, trade.volume, stop=True)
        self.getStopOrder(vtoid).unit = unit
        unit.atrStop = atrStopPrice

    def cancelOnTrade(self, direction):
        """
        在 onTrade 撤销停止单
        :param direction: CTAORDER_SHORT or CTAORDER_BUY
        :return:
        """
        for stopOrder in self.getAllStopOrders():
            # 找出开空的
            if stopOrder.direction == direction:
                # 撤单
                self.log.info(u'撤销 stopOrderID:{}'.format(stopOrder.stopOrderID))
                self.cancelOrder(stopOrder.stopOrderID)

                # unit 移除这个单
                # stopOrder.unit.removeStopOrder(stopOrder)

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

    def paramList2log(self):
        dic = OrderedDict()

        for k in self.paramList:
            dic[k] = getattr(self, k)

        msg = u''
        for k, v in dic.items():
            msg += u'{}:{} '.format(k, v)

        return msg

    def varList2Log(self):
        dic = OrderedDict({'close': self.bar1min.close})

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
        if self.pos != 0:
            # 已经有持仓了，不下开仓单
            self.log.debug(u'持仓中 pos:{} 不下开仓单'.format(self.pos))
            return
        else:
            pass
            # 空仓, 可以下开仓停止单

        # 先撤单,此时尚未开仓,可以全部撤单
        self.log.info(u'开仓挂单前先撤单')
        for stopOrder in self.getAllStopOrders():
            self.log.info(u'撤单 {}'.format(stopOrder))
            self.cancelOrder(stopOrder.stopOrderID)

        # 多头停止单
        self.log.info(u'挂多头停止单')
        hihgInPrices = [self.highIn1 + i * self.atr for i in range(self.unitsNum)]
        for i, price in enumerate(hihgInPrices):
            unit = self.unitList[i]
            # 连续下 unitsNum 个停止单
            stopOrderID = self.sendOrder(CTAORDER_BUY, price, self.hands, stop=True)
            # 保存订单号
            stopOrder = self.getStopOrder(stopOrderID)
            stopOrder.unit = unit
            unit.highIn = price
            unit.saveStopOrder(stopOrder)
            fomatter = {
                'unit': i + 1,
                'stopOrderID': stopOrderID,
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
            stopOrderID = self.sendOrder(CTAORDER_SHORT, price, self.hands, stop=True)
            # 保存订单号
            stopOrder = self.getStopOrder(stopOrderID)
            stopOrder.unit = unit
            unit.lowIn = price
            unit.saveStopOrder(stopOrder)
            fomatter = {
                'unit': i + 1,
                'stopOrderID': stopOrderID,
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

    def getLastUnit(self):
        """

        :param direction: DIRECTION_LONG OR DIRECTION_SHORT
        :return:
        """
        return self.unitList[-1]

    def saveStopOrder(self, stopOrder):
        self.stopOrders[stopOrder.stopOrderID] = stopOrder

    def onStopOrder(self, so):
        """
        收到停止单推送
        :param so:
        :return:
        """

        stopOrderID = so.stopOrderID
        self.log.info(u'停止单 {} '.format(so))

        if so.status == STOPORDER_WAITING:
            # u'等待中' 下单成功
            # 保存停止单
            self.saveStopOrder(so)
        elif so.status == STOPORDER_CANCELLED:
            # u'已撤销' 剔除该单号
            self.removeStopOrderID(stopOrderID)
            so.unit.removeStopOrder(so)
        elif so.status == STOPORDER_TRIGGERED:
            # u'已触发'
            if so.vtOrderID is None:
                self.log.error(u'没有返回 vtOrderID {}'.format(so.vtOrderID))
                raise ValueError()
            # 剔除该单号
            self.removeStopOrderID(stopOrderID)
            so.unit.removeStopOrder(so)

            # 触发成交，设置该仓位的目标持仓
            so.unit.setTargetPos(so.volume)
            # 保存 vtOrderID
            self.saveVtOrderID2Unit(so.vtOrderID, so.unit)
            so.unit.vtOrderID = so.vtOrderID

            self.log.info(u'{} vtOrderID {}'.format(so.unit, so.vtOrderID))
        else:  # 未知状态
            self.trading = False
            self.log.error(u'stopOrder 未知的停止单状态 {}'.format(so.status))
            for stopOrderID in self.getAllStopOrderIDs():
                self.cancelOrder(stopOrderID)

    def getAllStopOrderIDs(self):
        return list(self.stopOrders.keys())

    def getAllStopOrders(self):
        return list(self.stopOrders.values())

    def removeStopOrderID(self, stopOrderID):
        try:
            self.stopOrders.pop(stopOrderID)
            # self.log.info(u'移除 stopOrderID:{}'.format(stopOrderID))
        except KeyError:
            self.log.info(u'未找到可移除的 stopOrderID:{}'.format(stopOrderID))

    def getStopOrder(self, stopOrderID):
        return self.stopOrders.get(stopOrderID)

    def saveVtOrderID2Unit(self, vtOrderID, unit):
        self.vtOrderID2Unit[vtOrderID] = unit

    def getUnitByVtOrderID(self, vtOrderID):
        return self.vtOrderID2Unit.get(vtOrderID)

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        document = super(DonchianChannelStrategy, self).toSave()
        document.update({
            'pos': self.pos,
            'status': self.status,
            'unitList': []

        })

        # self.status = self.STATUS_EMPTY
        #
        # # 仓位列表
        # self.unitList = [Unit(self, i) for i in range(self.unitsNum)]  # 每一仓都是一个对象
        #
        # # stopOrderID: stopOrder
        # self.stopOrders = {}
        # self.vtOrderID2Unit = {}  # {vtOrderID : unit}

        return document

    def loadCtaDB(self, document):
        # todo 加载数据库中的 cta 策略数据
        self.log.info(u'{}'.format(str(document)))
        if document is None:
            self.log.info(u'没有可加载的存库数据')
            return

    def setStatus(self, status):
        """

        :param status: self.STATUS_EMPTY
        :return:
        """
        if status == self.status:
            # 没有进行实际变懂
            return
        self.log.info(u'{} -> {}'.format(self.status, status))
        self.status = status

    def varList2Html(self):
        orderDic = OrderedDict()
        for k in self.varList:
            v = getattr(self, k)
            if isinstance(v, float):
                try:
                    # 尝试截掉过长的浮点数
                    v = u'%0.1f' % v
                    while v.endswith('0'):
                        v = v[:-1]
                    if v.endswith('.'):
                        v = v[:-1]
                except:
                    pass
            orderDic[k] = v

        return orderDic

    def setOpenStatusOnOrder(self, unit, order, targetUnit, openStatus, orderStatus, fullStatus):
        """

        :param unit:
        :param order:
        :param targetUnit:
        :param openStatus:
        :param orderStatus:
        :param fullStatus:
        :return:
        """
        if unit is not targetUnit and self.status != fullStatus:
            # 状态更改为开多
            self.setStatus(openStatus)

        if unit is targetUnit and order.status == orderStatus:
            # 最后一仓，全部成交。更改状态为满仓多
            self.setStatus(fullStatus)

    def logProfileOnOrder(self, unit, order, orderStatus):
        # 该笔平仓单全部成交
        if order.status == orderStatus:
            # 汇报盈利
            self.log.info(u'该仓位盈利 {}'.format(unit.openCost - unit.closeCost))

            # 已经全部平仓
            if self.pos == 0 and sum([unit.pos for unit in self.unitList]) == 0:
                # 重置 unit
                # 输出总盈利
                self.log.info(u'多头结束')
                profile = sum([unit.openCost - unit.closeCost for unit in self.unitList])
                self.log.info(u'总盈利: {}'.format(profile))

    def resetAllUnit(self):
        """
        重置所有仓位的数据
        :return:
        """

        for u in self.unitList:
            u.reset()

    def isOpenLong(self, vtObject):
        assert isinstance(vtObject, VtOrderData) or isinstance(vtObject, VtTradeData)

        return vtObject.offset == OFFSET_OPEN and vtObject.direction == DIRECTION_LONG

    def isOpenShort(self, vtObject):
        assert isinstance(vtObject, VtOrderData) or isinstance(vtObject, VtTradeData)
        return vtObject.offset == OFFSET_OPEN and vtObject.direction == DIRECTION_SHORT

    def isCloseLong(self, vtObject):
        assert isinstance(vtObject, VtOrderData) or isinstance(vtObject, VtTradeData)

        return vtObject.offset in OFFSET_CLOSE_LIST and vtObject.direction == DIRECTION_SHORT

    def isCloseShort(self, vtObject):
        assert isinstance(vtObject, VtOrderData) or isinstance(vtObject, VtTradeData)

        return vtObject.offset in OFFSET_CLOSE_LIST and vtObject.direction == DIRECTION_LONG

    def toHtml(self):
        orderDic = super(DonchianChannelStrategy, self).toHtml()
        units = [u.toHtml() for u in self.unitList]
        orderDic['unit'] = pd.DataFrame(units).to_html()

        orderDic['bar'] = self.barToHtml()
        orderDic['bar1min'] = self.bar1minToHtml()
        return orderDic

    def barToHtml(self):
        if self.bar is None:
            return u'bar 无数据'
        itmes = (
            ('open', self.bar.open,),
            ('high', self.bar.high),
            ('low', self.bar.low),
            ('close', self.bar.close),
        )
        return OrderedDict(itmes)

    def bar1minToHtml(self):
        if self.bar1min is None:
            return u'bar1min 无数据'
        itmes = (
            ('open', self.bar1min.open,),
            ('high', self.bar1min.high),
            ('low', self.bar1min.low),
            ('close', self.bar1min.close),
        )
        return OrderedDict(itmes)

    def refreshSendStopOrder2CloseOnBar(self):
        """
        刷新止损单
        :return:
        """
        for u in self.unitList:
            # 刷新
