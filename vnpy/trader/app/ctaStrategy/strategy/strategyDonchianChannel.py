# encoding: UTF-8

"""
唐奇安通道交易策略
"""

import logging
import time
from collections import OrderedDict
from itertools import chain

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

    STATUS_EMPTY = u'空仓'
    STATUS_OPEN = u'建仓'
    STATUS_FULL = u'满仓'

    # 即将离场的状态
    STOP_STATUS_ATR = 'atr'
    STOP_STATUS_OUT = 'out'

    def __init__(self, strategy, number, direction):
        self.number = number
        self.strategy = strategy
        # 直接使用策略的句柄
        self.log = logging.getLogger(u'{}.unit{}{}'.format(strategy.vtSymbol, direction, number))
        self.log.parent = self.strategy.log

        self.direction = direction  # DIRECTION_LONG or DIRECTION_SHORT
        self.pos = 0  # 该unit的当前持仓
        self.maxPos = 0  # 该开仓阶段曾经达到的最大仓位
        self.targetPos = None  # 目标持仓
        self.openStopOrder = None  # 开仓停止单
        self.closeStopOrder = None  # 平仓停止单
        self.openTotalCost = 0
        self.closeTotalCost = 0
        self.openCostPrice = None  # 要使用 None 作为默认值
        self.closeCostPrice = None  # 要使用 None 作为默认值
        # self.status = self.STATUS_EMPTY
        self.atr = None  # 开仓时的 atr
        self.atrStopPrice = None  # atr 止损价格
        self.isFirst = False
        self.isLast = False
        self.dealVtTradIDs = set()
        self.dealVtOrderIDs = set()

    def logProfile(self):
        """
        计算盈利
        :return:
        """
        if self.maxPos == 0:
            err = u'该仓位尚未建仓'
            self.log.error(err)
            raise ValueError(err)

        # 每仓盈利
        if self.direction == DIRECTION_LONG:
            profile = self.closeTotalCost - self.openTotalCost
        else:
            profile = self.openTotalCost - self.closeTotalCost

        # self.log.warning(u'{}'.format(self.maxPos))
        # 每单位盈利
        handPro = round(profile / self.maxPos, 3)

        # 总盈利
        profile *= self.strategy.size

        self.log.info(u'盈利 {} 每单位 {}'.format(property, handPro))

    def setOpenCostPrice(self, costPrice):
        self.openCostPrice = costPrice

    def setCloseCostPrice(self, costPrice):
        self.closeCostPrice = costPrice

    @property
    def status(self):
        if self.targetPos is None:
            return self.STATUS_EMPTY
        if self.targetPos != self.pos:
            return self.STATUS_OPEN
        if self.targetPos == self.pos:
            return self.STATUS_FULL

    def toHtml(self):
        items = [
            ('num', self.number),
            ('direction', self.direction),
            ('pos', self.pos),
            ('targetPos', self.targetPos),
            ('status', self.status),
            ('atr', self.atr),
            ('atrStopPrice', self.atrStopPrice),
        ]

        if self.openStopOrder:
            items.append(('openPrice', self.openStopOrder.price))
        if self.closeStopOrder:
            items.append(('closePrice', self.closeStopOrder.price))

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

    def resetUnit(self):
        if self.pos != 0:
            self.log.warning(u'pos != 0')

        self.maxPos = 0

        self.targetPos = None

        if self.openStopOrder:
            # 待触发的停止单不撤销
            self.openStopOrder = None

        if self.closeStopOrder:
            # 待触发的停止单不撤销
            self.closeStopOrder = None

        self.openTotalCost = 0
        self.closeTotalCost = 0

        # self.costPrice = None
        self.setOpenCostPrice(None)
        # self.openCostPrice = None
        self.setCloseCostPrice(None)
        # self.closeCostPrice = None
        # self.atr = None
        self.setAtr(None)
        self.atrStopPrice = None

    def __str__(self):
        s = u'< Unit.{} '.format(self.number)
        s += u'dir:{} '.format(self.direction)
        s += u'pos:{}/{} '.format(self.pos, self.targetPos)
        s += u'status:{} '.format(self.status)
        s += u'atr:{} '.format(self.atr)
        s += u'>'
        return s

    @property
    def empty(self):
        """
        是否空仓
        :return:
        """
        return self.pos == 0

    def setTargetPos(self, targetPos):
        self.log.info(u'targetPos {} -> {}'.format(self.targetPos, targetPos))
        self.targetPos = targetPos

    def setAtr(self, atr):
        self.log.info(u'{} -> {}'.format(self.atr, atr))
        self.atr = atr

    def saveVtTrade(self, vtTrade):
        assert isinstance(vtTrade, VtTradeData)
        if not self.strategy.isBackTesting():
            # 回测时不需要缓存 vtTrade
            self.dealVtTradIDs.add(vtTrade.vtTradeID)

        try:
            # 计算成本
            if vtTrade.offset == OFFSET_OPEN:
                # 开仓成本
                self.pos += vtTrade.volume
                # 设置仓位
                self.maxPos = max(self.pos, self.maxPos)
                self.openTotalCost += vtTrade.price * vtTrade.volume
                # 单价成本
                self.setOpenCostPrice(self.openTotalCost / self.pos)
            elif vtTrade.offset in OFFSET_CLOSE_LIST:
                # 平仓成本
                self.pos -= vtTrade.volume
                self.closeTotalCost += vtTrade.price * vtTrade.volume
                # 单价成本
                self.setCloseCostPrice(self.closeTotalCost / (self.maxPos - self.pos))
            else:
                err = u'未知的仓位方向 {}'.format(vtTrade.offset)
                self.log.error(err)
                raise ValueError(err)
        except ZeroDivisionError:
            self.log.error(u'{} 异常的成交数量 trade.volume:{}'.format(self, vtTrade.volume))

    def waskDealVtTradeID(self, vtTradeID):
        """

        :param vtTradeID: vtTrade.vtTradeID
        :return:
        """
        return vtTradeID in self.dealVtTradIDs

    def saveVtOrder(self, vtOrder):
        assert isinstance(vtOrder, VtOrderData)
        if not self.strategy.isBackTesting():
            self.dealVtOrderIDs.add(vtOrder.vtOrderID)

    def waskDealVtOrderID(self, vtOrderID):
        """

        :param vtOrderID: vtOrder.vtOrderID
        :return:
        """
        return vtOrderID in self.dealVtTradIDs

    def toSave(self):
        """
        要存库的数据
        :return:
        """
        dic = {
            'pos': self.pos,  # 该unit的当前持仓
            'maxPos': self.maxPos,  # 该开仓阶段曾经达到的最大仓位
            'targetPos': self.targetPos,  # 目标持仓
            # self.openStopOrder = None  # 开仓停止单
            # self.closeStopOrder = None  # 平仓停止单
            'openTotalCost': self.openTotalCost,
            'closeTotalCost': self.closeTotalCost,
            'openCostPrice': self.openCostPrice,  # 要使用 None 作为默认值
            'closeCostPrice': self.closeCostPrice,  # 要使用 None 作为默认值
            # self.status = self.STATUS_EMPTY
            'atr': self.atr,  # 开仓时的 atr
            'atrStopPrice': self.atrStopPrice,  # atr 止损价格
            'dealVtTradIDs': list(self.dealVtTradIDs),
            'dealVtOrderIDs': list(self.dealVtOrderIDs),
        }
        return dic

    def fromSave(self, document):
        """
        读取数据
        :param document:
        :return:
        """
        for k in self.toSave().keys():
            key = u'u.{}.{}.{}'.format(self.direction, self.number, k)
            v = document.get(key, getattr(self, k))
            setattr(self, k, v)

        self.dealVtTradIDs = set(self.dealVtTradIDs)
        self.dealVtOrderIDs = set(self.dealVtOrderIDs)


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
    barPeriod = 15  # min 一根bar是什么周期的

    # 策略变量
    bar = None  # K线对象
    barList = []  # K线对象的列表

    atrPeriod = 14
    unitsNum = 4  # 一共4仓
    risk = 0.02  # 账户风险投入
    maxCD = 1  # 最大冷却次数
    sys2Vaild = True  # 是否启用系统2

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'atrPeriod',
        'unitsNum',
        'hands',
        'maxCD',
        'sys2Vaild',
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

    hands = 1  # 每仓多少手

    cd = 0  # 冷却中，不下单
    sys2 = False

    # 变量列表，保存了变量的名称
    varList = CtaTemplate.varList[:]
    varList.extend([
        'status',
        'cd',
        'sys2',

        'highIn1',
        'highOut1',
        'lowIn1',
        'lowOut1',
        'highIn2',
        'highOut2',
        'lowIn2',
        'lowOut2',

        'atr',
    ])

    indexList = [
        'highIn1',
        'highOut1',
        'lowIn1',
        'lowOut1',
        'highIn2',
        'highOut2',
        'lowIn2',
        'lowOut2',
    ]

    INDEX_STATUS_EMPTY = u'空仓'  # 策略状态，空仓
    INDEX_STATUS_OPEN = u'建仓'  # 建仓中
    INDEX_STATUS_FULL = u'满仓'  # 满仓

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DonchianChannelStrategy, self).__init__(ctaEngine, setting)

        self.maxBarNum = max(self.in1, self.out1, self.in2, self.out2, self.atrPeriod)  # 最大的入场周期
        self.barCount = 0
        self.barList = []
        self.highList = []  # 最高价队列
        self.lowList = []  # 最低价队列
        self.closeList = []  # 收盘价队列

        self.status = self.INDEX_STATUS_EMPTY

        # 仓位列表
        self.longUnitList = [Unit(self, i, DIRECTION_LONG) for i in range(self.unitsNum)]  # 每一仓都是一个对象
        self.longUnitList[0].isFirst = True
        self.longUnitList[-1].isLast = True

        self.shortUnitList = [Unit(self, i, DIRECTION_SHORT) for i in range(self.unitsNum)]  # 每一仓都是一个对象
        self.shortUnitList[0].isFirst = True
        self.shortUnitList[-1].isLast = True

        self.vtOrderID2Unit = {}  # {vtOrderID: unit}
        self.stopOrders = {}  # {stopOrderID: unit}

        self.isRefreshOpenPrice = False

        self.totalPos = 0

    @property
    def highIn(self):
        return self.highIn2 if self.sys2 else self.highIn1

    @property
    def lowIn(self):
        return self.lowIn2 if self.sys2 else self.lowIn1

    @property
    def highOut(self):
        return self.highOut2 if self.sys2 else self.highOut1

    @property
    def lowOut(self):
        return self.lowOut2 if self.sys2 else self.lowOut1

    def onInit(self):
        if self.unitsNum == 0:
            self.log.error(u'unitsNum == '.format(self.unitsNum))
        self.log.info(self.paramList2log())

        # for u in self.unitList:
        #     u.clearDealVtOrderIDs()

        initData = self.loadBar(self.maxBarNum)
        self.log.info(u'即将加载 {} 个 bar'.format(len(initData)))
        initData.sort(key=lambda bar: bar.datetime)
        if not initData:
            self.log.warning(u'没有任何历史数据')
            return
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

        for bar1min in initData:
            self.bar1min = bar1min
            self.onBar(bar1min)

        # if __debug__:
        #     self.log.debug(u'强制入场')
        #     self.highIn1 = 4330

        if len(self.barList) >= self.maxBarNum:
            # 计算出入场价格
            self._calIndexValue()

        if __debug__ and self.status == self.INDEX_STATUS_EMPTY:
            self.log.info(self.varList2Log())

        if len(initData) >= self.maxBarNum:
            self.log.info(u'初始化完成')
        else:
            self.log.warning(u'初始化数据不足，初始化失败!')
            self.inited = False

        # 从数据库加载策略数据
        if not self.isBackTesting():
            # 需要等待保证金加载完毕
            document = self.fromDB()
            self.loadCtaDB(document)

        self.putEvent()

    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.log.info(u'策略 {} 启动'.format(self.className))

        if len(self.barList) >= self.maxBarNum:
            if self.status == self.INDEX_STATUS_EMPTY:
                # 更新仓位大小
                self.updateHands()

            # 启动后，挂停止单挂停止单
            self.sendOpenStopOrder()

        if self.isBackTesting() and len(self.barList) < self.maxBarNum:
            self.trading = False

        self.putEvent()

    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.log.info(u'策略 {} 停止'.format(self.className))
        self.putEvent()
        self.saveDB()

    def onTick(self, tick):
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:
            if self.bar1min:
                self.onBar(self.bar1min)

            self.bar1min = self.newBar(tick)
            self.barMinute = tickMinute  # 更新当前的分钟

        else:  # 否则继续累加新的K线
            self.refreshBarByTick(self.bar1min, tick)

            # 开仓之后，要实时更新入场位置
            if self.status != self.INDEX_STATUS_EMPTY:
                self.highIn1 = max(self.highIn1, tick.lastPrice)
                self.highIn2 = max(self.highIn2, tick.lastPrice)
                self.lowIn1 = min(self.lowIn1, tick.lastPrice)
                self.lowIn2 = min(self.lowIn2, tick.lastPrice)

    def onBar(self, bar1min):
        """

        :param bar1min:a
        :return:
        """
        CtaTemplate.onBar(self, bar1min)

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
        # self.barCount = len(self.barList)

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
        # if varLogPre != varLogLater:
        #     self.log.info(varLogLater)

        # 使用停止单，下开仓单。仅在 INDEX_STATUS_EMPTY 时有效
        if self.status == self.INDEX_STATUS_EMPTY:
            self.sendOpenStopOrder()

        # 已经开仓了，刷新止损单的止损价
        if self.status in (self.INDEX_STATUS_OPEN, self.INDEX_STATUS_FULL):
            self.refreshCloseStopOrderOnBar()

        self.saveDB()

    def onOrder(self, order):
        """

        vtOrder.status:
            STATUS_NOTTRADED = u'未成交'
            STATUS_PARTTRADED = u'部分成交'
            STATUS_ALLTRADED = u'全部成交'
            STATUS_CANCELLED = u'已撤销'
            STATUS_REJECTED = u'拒单'
            STATUS_UNKNOWN = u'未知'

        :param vtOrder:
        :return:
        """

        assert isinstance(order, VtOrderData)

        vtOrderID = order.vtOrderID

        self.log.info(u'vtOrder:{} status: {}'.format(vtOrderID, order.status))

        unit = self.getUnitByVtOrderID(vtOrderID)

        if unit is None:
            # self.log.warning(u'订单 {} 没有对应的 unit'.format(vtOrderID))
            return

        assert isinstance(unit, Unit)

        if unit.waskDealVtOrderID(vtOrderID):
            self.log.waring(u'unit {} 已经处理完成'.format(unit))
            return

        # 根据订单状态进行处理
        if order.status == STATUS_NOTTRADED:
            # u'未成交'
            pass

        elif order.status == STATUS_PARTTRADED:
            # u'部分成交'
            pass

        elif order.status == STATUS_ALLTRADED:
            # u'全部成交'
            #
            unit.saveVtOrder(order)

        elif order.status == STATUS_CANCELLED:
            # u'已撤销'
            unit.saveVtOrder(order)

        elif order.status == STATUS_REJECTED:
            # u'拒单'
            unit.saveVtOrder(order)

        elif order.status == STATUS_UNKNOWN:
            # u'未知'
            pass
        else:
            err = u'无法确定订单状态 {}'.format(order.status)
            self.log.error(err)
            raise ValueError(err)

        # 发出状态更新事件
        self.putEvent()
        self.saveDB()

    def onTrade(self, trade):
        """
        可能会是部分成交
        :param trade: gateWay 订单实例
        :return:
        """
        assert isinstance(trade, VtTradeData)
        unit = self.getUnitByVtOrderID(trade.vtOrderID)

        # 这个订单是否已经被处理过了
        if unit.waskDealVtTradeID(trade.vtTradeID):
            self.log.warning(u'收到重复的 tradeID: {}'.format(trade.tradeID))
            return

        self.log.info(
            u'{} 成交 vtOrderID:{} vtTradeID:{} price: {} volume: {}'.format(trade.direction, trade.vtTradeID,
                                                                           trade.tradeID,
                                                                           trade.price, trade.volume))

        # 保存已经处理过的 trade 对象
        # 记录成交成本和已经成交的仓位
        # 后面的计算要依赖于此保存
        unit.saveVtTrade(trade)

        # self.log.warning(u'{}'.format(unit))
        # self.log.warning(u'{}'.format(unit.maxPos))

        if trade.offset == OFFSET_OPEN:
            # 开仓
            # 手续费
            self.charge(trade.offset, trade.price, trade.volume)

            # 回测时滑点
            if self.isBackTesting():
                self.chargeSplipage(trade.volume)

            # 可用资金变动
            self.capital -= trade.volume * trade.price * self.marginRate

            self._handleOpenOnTrade(trade)

        if trade.offset in OFFSET_CLOSE_LIST:
            # 平仓
            # 手续费
            self.charge(trade.offset, trade.price, trade.volume)

            # 回测时滑点
            if self.isBackTesting():
                self.chargeSplipage(trade.volume)

            self._handlCloseOnTrade(unit, trade)

        # 发出状态更新事件
        self.putEvent()
        self.saveDB()

    def _handleOpenOnTrade(self, vtTrade):
        """
        处理开仓成交的逻辑
        :param vtTrade:
        :return:
        """
        assert isinstance(vtTrade, VtTradeData)

        # 更新止损单
        if vtTrade.direction == DIRECTION_LONG:
            # 多头
            self._refreshCloseStopOrder(self.longUnitList)
        else:
            # 空头
            self._refreshCloseStopOrder(self.shortUnitList)

    def _handlCloseOnTrade(self, unit, vtTrade):
        # 平仓完成，重新下开仓单
        if self.status == self.INDEX_STATUS_EMPTY and unit.isFirst:
            for u in chain(self.longUnitList, self.shortUnitList):
                assert isinstance(u, Unit)
                assert isinstance(unit, Unit)
                if not u.empty:
                    b = self.bar1min
                    self.log.warning(u'{} {} {} {} '.format(b.open, b.high, b.low, b.close))
                    self.log.warning(u'当前平仓 {}'.format(unit))
                    self.log.warning(u'{}'.format(unit.closeStopOrder))
                    self.log.warning(u'平仓未完成 {}'.format(u))
                    self.log.warning(u'{}'.format(u.closeStopOrder))
                    break
            else:
                self._closeFinish(unit)

    def _closeFinish(self, unit):
        self.log.info(u'首仓平仓完成')
        # 计算盈利
        self.log.info(u'计算盈利')

        # 汇总
        openTotalCost = 0  # 开仓总价
        closeTotalCost = 0  # 平仓总价
        pos = 0  # 仓位
        for u in self.getUnitListByDirection(unit.direction):
            if u.status == u.STATUS_FULL:
                # 该仓位需要建仓过才能汇总利润
                u.logProfile()
                openTotalCost += u.openTotalCost
                closeTotalCost += u.closeTotalCost
                pos += u.maxPos

        if unit.direction == DIRECTION_LONG:
            profile = closeTotalCost - openTotalCost
        else:
            profile = openTotalCost - closeTotalCost

        # 每单位盈利
        handPro = self.roundToPriceTick(profile / pos)

        # 总盈利
        profile *= self.size

        self.capital += profile

        self.log.info(u'汇总')
        self.log.info(u'盈利 {} 每单位 {}'.format(profile, handPro))

        self.log.info(u'重新下开仓单')
        # 没有开仓，直接可以开始
        self.resetAllUnit()
        self.sendOpenStopOrder()

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

        for k in self.indexList:
            price = getattr(self, k)
            setattr(self, k, self.roundToPriceTick(price))

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

    def sendOpenStopOrder(self):
        """
        挂开仓的停止单
        :return:
        """
        self.updateHands()
        self.sendLongOpenStopOrder()
        self.sendShortOpenStopOrder()
        if self.isRefreshOpenPrice:
            self.log.info(u'挂开仓停止单完成')
            self.isRefreshOpenPrice = False

    def sendLongOpenStopOrder(self):
        # 多头停止单
        atr = self.atr
        openPrice = self.highIn

        # 下开仓单
        if self.sys2Vaild and openPrice == self.highIn2:
            # 使用系统2，且有可能进入系统2
            hands = self.hands
        else:
            hands = 0 if self.cd else self.hands

        for unit in self.longUnitList:
            if unit.status != unit.STATUS_EMPTY:
                continue
            # 多仓
            # ========================
            if unit.openStopOrder:
                assert isinstance(unit.openStopOrder, StopOrder)
                # 已经下过开单了
                if unit.openStopOrder.price != openPrice:
                    self.log.info(u'更新开仓单 {}'.format(unit))
                    self.log.info(u'{} -> {}'.format(unit.openStopOrder.price, openPrice))
                    unit.openStopOrder.price = openPrice
                    self.isRefreshOpenPrice = True
            else:
                # 还没开仓过
                self.log.info(u'开仓下单')
                stopOrderID = self.sendOrder(CTAORDER_BUY, openPrice, hands, stop=True)
                stopOrder = self.getStopOrderByStopID(stopOrderID)
                stopOrder.unit = unit
                stopOrder.priority = unit.number
                unit.openStopOrder = stopOrder
                self.isRefreshOpenPrice = True

            # 计算下一档价位
            diff = self.unitPriceDiff(atr)

            # if __debug__:
            #     self.log.debug(u'shortOpenPrice {}'.format(openPrice))
            #     self.log.debug(u'atr {}'.format(atr))
            #     self.log.debug(u'diff {}'.format(diff))

            openPrice += diff
            openPrice = self.roundToPriceTick(openPrice)

    def sendShortOpenStopOrder(self):
        atr = self.atr
        openPrice = self.lowIn

        if self.sys2Vaild and openPrice == self.lowIn2:
            # 使用系统2，且有可能进入系统2
            hands = self.hands
        else:
            hands = 0 if self.cd else self.hands

        # 下开仓单
        for unit in self.shortUnitList:
            if unit.status != unit.STATUS_EMPTY:
                continue

            # 空仓
            # ===========================
            if unit.openStopOrder:
                assert isinstance(unit.openStopOrder, StopOrder)
                if unit.openStopOrder.price != openPrice:
                    self.log.info(u'更新开仓单 {}'.format(unit))
                    self.log.info(u'{} -> {}'.format(unit.openStopOrder.price, openPrice))
                    unit.openStopOrder.price = openPrice
                    self.isRefreshOpenPrice = True
            else:
                self.log.info(u'开仓下单')
                stopOrderID = self.sendOrder(CTAORDER_SHORT, openPrice, hands, stop=True)
                stopOrder = self.getStopOrderByStopID(stopOrderID)
                stopOrder.unit = unit
                stopOrder.priority = unit.number
                unit.openStopOrder = stopOrder
                self.isRefreshOpenPrice = True

            # 计算下一档价位
            diff = self.unitPriceDiff(atr)
            # if __debug__:
            #     self.log.debug(u'openPrice {}'.format(openPrice))
            #     self.log.debug(u'atr {}'.format(atr))
            #     self.log.debug(u'diff {}'.format(diff))
            openPrice -= diff
            openPrice = self.roundToPriceTick(openPrice)

    def getStopOrderByStopID(self, stopOrderID):
        return self.stopOrders.get(stopOrderID)

    def unitPriceDiff(self, atr):
        """
        两个 相邻unit 之间的价差
        :return:
        """
        # if __debug__:
        #     self.log.debug('{}'.format(self.stopAtr))
        #     self.log.debug('{}'.format(self.stopAtr * 1.))
        #     self.log.debug('{}'.format(self.unitsNum))
        #     self.log.debug('{}'.format(self.stopAtr * 1. / self.unitsNum))
        #     self.log.debug('{}'.format(atr))
        #     self.log.debug('{}'.format(atr * self.stopAtr * 1. / self.unitsNum))

        return atr * self.stopAtr * 1. / self.unitsNum

    def saveStopOrder(self, stopOrder):
        self.stopOrders[stopOrder.stopOrderID] = stopOrder

    def onStopOrder(self, so):
        """
        收到停止单推送
        :param so:
        :return:
        """
        isinstance(so, VtStopOrder)

        stopOrderID = so.stopOrderID

        self.log.info(u'停止单 {} '.format(so))

        if so.status == STOPORDER_WAITING:
            # u'等待中'
            self.log.info(u'下单成功')
            # 保存停止单
            self.saveStopOrder(so)
            return
        elif so.status == STOPORDER_TRIGGERED:
            # u'已触发'
            self.onStopOrderTriggered(so)

        elif so.status == STOPORDER_CANCELLED:
            # u'已撤销' 剔除该单号
            self.log.info(u'撤单完成,移除单号')
            self.removeStopOrder(stopOrderID)
        else:  # 未知状态
            err = u'未知的停止单状态 {}'.format(so)
            self.log.error(err)
            raise ValueError(err)

        self.saveDB()

    def loadCtaDB(self, document):
        # 加载数据库中的 cta 策略数据
        if document is None:
            self.log.info(u'没有可加载的存库数据')
            return

        super(DonchianChannelStrategy, self).loadCtaDB(document)

        self.status = document['status']
        self.cd = document['cd']
        self.sys2 = document['sys2']

        for dire in [DIRECTION_LONG, DIRECTION_SHORT]:
            for u in self.getUnitListByDirection(dire):
                u.fromSave(document)
                #         for k, v in dic.items():
                #             v = document[u'u.{}.{}.{}'.format(DIRECTION_LONG, u.number, k)]
                #             setattr(k, v)

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        document = super(DonchianChannelStrategy, self).toSave()
        document.update({
            'status': self.status,
            'cd': self.cd,
            'sys2': True,
        })
        for dire in [DIRECTION_LONG, DIRECTION_SHORT]:
            for u in self.getUnitListByDirection(dire):
                dic = u.toSave()
                for k, v in dic.items():
                    document[u'u.{}.{}.{}'.format(DIRECTION_LONG, u.number, k)] = v

        # self.status = self.STATUS_EMPTY
        #
        # # 仓位列表
        # self.unitList = [Unit(self, i) for i in range(self.unitsNum)]  # 每一仓都是一个对象
        #
        # # stopOrderID: stopOrder
        # self.stopOrders = {}
        # self.vtOrderID2Unit = {}  # {vtOrderID : unit}

        return document

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

    def resetAllUnit(self):
        """
        重置所有仓位的数据
        :return:
        """
        for u in chain(self.longUnitList, self.shortUnitList):
            if u.openStopOrder:
                # 撤掉所有开仓单
                self.cancelOrder(u.openStopOrder.stopOrderID)
            u.resetUnit()

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
        units = [u.toHtml() for u in chain(reversed(self.longUnitList), self.shortUnitList)]
        orderDic['unit'] = pd.DataFrame(units).to_html()
        return orderDic

    def setStatusOnStopOrder(self, so):
        """
        
        :param so: Vt
        :return: 
        """
        assert isinstance(so, StopOrder)

        # 开仓
        if so.offset == OFFSET_OPEN:
            # 更改状态 空仓 -> 开仓 -> 满仓 -> 空仓
            if self.status == self.INDEX_STATUS_EMPTY:
                if so.unit.isLast:
                    # 最后一仓，直接满仓
                    self.setStatus(self.INDEX_STATUS_FULL)
                else:
                    # 大小周期切换，要在更改状态前。尝试进入系统2
                    self.switchSysOnStopOrder(so)

                    # 设为开仓
                    self.setStatus(self.INDEX_STATUS_OPEN)
            elif self.status == self.INDEX_STATUS_OPEN and so.unit.isLast:
                # 最后一仓也触发了，满仓状态
                self.setStatus(self.INDEX_STATUS_FULL)

        # 平仓
        if so.stopOrderID == 'CtaStopOrder.3':
            self.log.debug(u'{}'.format(self.status))
            self.log.debug(u'{}'.format(so.unit.isFirst))

        if so.offset in OFFSET_CLOSE_LIST:
            if self.status in (self.INDEX_STATUS_OPEN, self.INDEX_STATUS_FULL) and so.unit.isFirst:
                # 建仓/满仓状态，首仓触发了，是完全平仓了
                self.setStatus(self.INDEX_STATUS_EMPTY)
                # 平仓，退出系统2
                self.deactiveSys2()

    def switchSysOnStopOrder(self, so):
        """
        大小周期切换
        :param so:
        :return:
        """
        assert isinstance(so, StopOrder)

        if self.status == self.INDEX_STATUS_EMPTY and so.offset == OFFSET_OPEN and so.unit.isFirst:
            # 还处于空仓中， 是开仓停止单, 是第一仓
            if so.direction == DIRECTION_LONG:
                # 开多仓时
                sys2 = so.price == self.highIn1 == self.highIn2
            else:
                # 开空仓时
                sys2 = so.price == self.lowIn1 == self.lowIn2

            if sys2:
                # 进入系统2
                self.activeSys2()
                # 重置CD为0
                self.clearCD()

    def setUnitAtrOnStopOrder(self, so):
        if self.status == self.INDEX_STATUS_EMPTY:
            return
        assert isinstance(so, StopOrder)

        if so.offset != OFFSET_OPEN:
            return

        unitList = self.getUnitListByDirection(so.unit.direction)

        self.log.info(u'设置 atr {}'.format(self.atr))

        for unit in unitList:
            if unit.atr is None:
                unit.setAtr(self.atr)

    def setUnitTargetPosOnStopOrder(self, so):
        assert isinstance(so, StopOrder)
        # 触发成交，设置该仓位的目标持仓
        unit = so.unit
        if so.offset in OFFSET_CLOSE_LIST:
            # 触发了平仓
            unit.setTargetPos(0)
        elif so.offset == OFFSET_OPEN:
            # 触发了开仓或者满仓
            unit.setTargetPos(so.volume)
        else:
            err = u'未知的停止单操作 {}'.format(so.offset)
            self.log.error(err)

    def removeStopOrder(self, stopOrderID):
        """
        从缓存中移除一个停止单
        :param stopOrderID:
        :return:
        """
        try:
            stopOrder = self.stopOrders.pop(stopOrderID)
            self.log.info(u'移除 {}'.format(stopOrder))
        except KeyError:
            self.log.info(u'移除失败 stopOrderID: {} '.format(stopOrderID))

    def cancelOnStopOrder(self, so):
        assert isinstance(so, StopOrder)
        if so.offset == OFFSET_OPEN and self.status != self.INDEX_STATUS_EMPTY:
            self.log.info(u'反向开仓单撤单')
            # 获得反向的仓位列表
            direciont = self.getReverseDirection(so.direction)
            unitList = self.getUnitListByDirection(direciont)
            for unit in unitList:
                stopOrder, unit.openStopOrder = unit.openStopOrder, None
                if stopOrder is not None:
                    # 撤单
                    self.cancelOrder(stopOrder.stopOrderID)

    def sendOpenStopOrderOnStopOrder(self, so):
        assert isinstance(so, StopOrder)

        for u in chain(self.longUnitList, self.shortUnitList):
            if not u.empty:
                return

        # 没有开仓，直接可以开始
        self.resetAllUnit()

        self.sendOpenStopOrder()

    def getUnitListByDirection(self, direction):
        return self.longUnitList if direction == DIRECTION_LONG else self.shortUnitList

    def getReverseDirection(self, direction):
        return DIRECTION_LONG if direction == DIRECTION_SHORT else DIRECTION_SHORT

    def refreshCloseStopOrderOnBar(self):
        """
        刷新停止单的价格
        :return:
        """
        # 多头
        self._refreshCloseStopOrder(self.longUnitList)

        # 空头
        self._refreshCloseStopOrder(self.shortUnitList)

    def _refreshCloseStopOrder(self, unitList):
        # 已经开仓的头寸个数
        openNum = 0
        reverseList = reversed(unitList)
        for unit in reverseList:

            if unit.closeStopOrder is None or unit.closeStopOrder.status != STOPORDER_WAITING:
                # 没有停止单
                # 等待成交中的才更改止损价
                continue

            volume = unit.pos
            costPrice = unit.openCostPrice  # 成交成本价

            atrDiff = openNum * (self.stopAtr / self.unitsNum * unit.atr)
            if unit.direction == DIRECTION_SHORT:
                costPrice -= atrDiff
            else:
                costPrice += atrDiff

            prePrice = unit.closeStopOrder.price

            self._sendCloseStopOrder(unit, costPrice, volume)
            if unit.closeStopOrder.price != prePrice:
                self.log.info(u'更新止损单价格 ')
                self.log.info(u'{}'.format(unit))
                self.log.info(u'{}'.format(unit.closeStopOrder))
                self.log.info(u'{} -> {}'.format(prePrice, unit.closeStopOrder.price))

            openNum += 1

        # 再反转
        # 后一仓的止损价，不能优先于前一仓
        prePrice = None
        for unit in unitList:
            if unit.closeStopOrder is None or unit.closeStopOrder.status != STOPORDER_WAITING:
                # 没有停止单
                # 等待成交中的才更改止损价
                continue
            if prePrice is not None:
                if unit.direction == DIRECTION_LONG:
                    # 多头
                    if unit.closeStopOrder.price < prePrice:
                        self.log.info(u'限制止损单价格 {} {} -> {}'.format(unit, prePrice, unit.closeStopOrder.price))
                        unit.closeStopOrder.price = prePrice
                else:
                    # 空头
                    if unit.closeStopOrder.price > prePrice:
                        self.log.info(u'限制止损单价格 {} {} -> {}'.format(unit, prePrice, unit.closeStopOrder.price))
                        unit.closeStopOrder.price = prePrice

            prePrice = unit.closeStopOrder.price

    def sendCloseStopOrderOnStopOrder(self, so):
        assert isinstance(so, StopOrder)

        if so.offset in OFFSET_CLOSE_LIST:
            # 平仓中，不需要下止损单
            return

        if self.status == self.INDEX_STATUS_EMPTY:
            # 系统状态为空仓，不需要下停止单
            return

        self.log.info(u'开仓后下止损单')
        unit = so.unit
        assert isinstance(unit, Unit)

        volume = 0  # 下单手数将会在开仓的成交回调中更新
        self._sendCloseStopOrder(unit, unit.openCostPrice, volume)

        # 刷新其他仓位的止损价
        # 多头
        self._refreshCloseStopOrder(self.longUnitList)
        # 空头
        self._refreshCloseStopOrder(self.shortUnitList)

    def _sendCloseStopOrder(self, unit, costPrice, volume):
        # 根据给出的成本价，计算止损单价格
        if unit.direction == DIRECTION_LONG:
            # 计算 2atr 止损价，并更新
            atrStopPrice = self.roundToPriceTick(costPrice - self.stopAtr * unit.atr)
            # 对比离场价和止损价
            outPrice = max(atrStopPrice, self.highOut)
            ctaOrderOffset = CTAORDER_SELL
        elif unit.direction == DIRECTION_SHORT:
            # 计算 2atr 止损价，并更新
            atrStopPrice = self.roundToPriceTick(costPrice + self.stopAtr * unit.atr)
            # 对比离场价和止损价
            outPrice = min(atrStopPrice, self.lowOut)
            ctaOrderOffset = CTAORDER_COVER
        else:
            msg = u'未知的开仓方向'
            self.log.error(msg)
            raise ValueError(msg)

        unit.atrStopPrice = atrStopPrice

        outPrice = self.roundToPriceTick(outPrice)

        if unit.closeStopOrder:
            # 已经下过单了
            # 直接更新下单价格
            unit.closeStopOrder.price = outPrice
            unit.closeStopOrder.volume = volume
        else:
            self.log.info(u'下止损单 {}'.format(outPrice))
            # 还没下单过
            stopOrderID = self.sendOrder(ctaOrderOffset, outPrice, volume, stop=True)
            stopOrder = self.getStopOrderByStopID(stopOrderID)
            stopOrder.unit = unit
            stopOrder.priority = unit.number
            unit.closeStopOrder = stopOrder
            self.log.info(u'下止损单后 {}'.format(stopOrder.price))

    def setUnitCostPriceOnStopOrder(self, so):
        unit = so.unit
        assert isinstance(unit, Unit)

        if so.offset == OFFSET_OPEN:
            if unit.openCostPrice is None:
                unit.setOpenCostPrice(so.price)
        elif so.offset in OFFSET_CLOSE_LIST:
            if unit.closeCostPrice is None:
                unit.setCloseCostPrice(so.price)
        else:
            err = u'未知的仓位方向 {}'.format(so.offset)
            self.log.error(err)
            raise ValueError(err)

    def saveVtOrderID2Unit(self, vtOrderID, unit):
        self.vtOrderID2Unit[vtOrderID] = unit

    def getUnitByVtOrderID(self, vtOrderID):
        return self.vtOrderID2Unit.get(vtOrderID)

    def onStopOrderTriggered(self, so):
        if so.unit is None:
            msg = u'一个触发的stopOrder {} 没有绑定 unit'
            self.log.error(msg)
            raise ValueError(msg)
        stopOrderID = so.stopOrderID
        # 剔除该单号
        self.removeStopOrder(stopOrderID)

        # 更改状态
        self.setStatusOnStopOrder(so)

        # 设置 unit.atr
        self.setUnitAtrOnStopOrder(so)

        # 设置目标持仓
        self.setUnitTargetPosOnStopOrder(so)

        # 设置最初建仓成本价
        self.setUnitCostPriceOnStopOrder(so)

        # 同时撤掉反向的开仓单
        self.cancelOnStopOrder(so)

        # 下止损单
        self.sendCloseStopOrderOnStopOrder(so)

        # 设置盈利离场冷却
        self.setCDOnStopOrder(so)

        # 平仓完毕，重新下开仓单
        if self.status == self.INDEX_STATUS_EMPTY and so.unit.isFirst and so.offset in OFFSET_CLOSE_LIST:
            self.log.info(u'首仓平仓完成,重新下开仓单')
            self.sendOpenStopOrderOnStopOrder(so)

        if so.volume != 0:
            # 保存 vtOrderID
            self.saveVtOrderID2Unit(so.vtOrderID, so.unit)
            so.unit.vtOrderID = so.vtOrderID
            self.log.info(u'{} vtOrderID {}'.format(so.unit, so.vtOrderID))

    def updateHands(self):
        """
        更新开仓仓位
        :return:
        """
        try:
            minHands = max(0, int(self.capital * self.risk / (self.size * self.atr * self.stopAtr)))

            maxHands = max(0, int(
                self.capital * 0.95 / (
                    self.size * self.unitsNum * self.bar1min.close * self.marginRate)))
            self.hands = min(minHands, maxHands)

        except TypeError as e:
            if 'NoneType' in e.message:
                pass
        except ValueError as e:
            self.log.warning(u'b{} r{} a{} s{} h{}'.format(self.capital, self.risk, self.atr, self.size, self.hands))
            raise
        except  ZeroDivisionError as e:
            self.log.warning(u'{} {} {} {}'.format(self.size, self.unitsNum, self.bar1min.close, self.marginRate))
            raise

    def setCDOnStopOrder(self, so):
        assert isinstance(so, StopOrder)

        if so.offset not in OFFSET_CLOSE_LIST:
            return

        unit = so.unit
        preCd = self.cd
        if unit.atrStopPrice == so.price:
            # atr止损 减少 cd
            self.cd = max(self.cd - 1, 0)
        else:
            # 离场，设置 CD
            self.cd = self.maxCD

            # self.log.info(u'{}'.format(unit))
            # self.log.info(u'{} {}'.format(unit.atrStopPrice, so.price))
            # self.log.info(u'CD {} -> {}'.format(preCd, self.cd))

    def activeSys2(self):
        if self.sys2Vaild:
            self.log.info(u'进入系统2')
            self.sys2 = True

    def deactiveSys2(self):
        if self.sys2Vaild:
            self.log.info(u'退出系统2')
        self.sys2 = False

    def clearCD(self):
        self.cd = 0
