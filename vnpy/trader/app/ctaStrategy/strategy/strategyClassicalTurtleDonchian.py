# encoding: UTF-8




from threading import Timer
from collections import OrderedDict
import time

import arrow
import pandas as pd

from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue, exception, logDate
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBase import *

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class ClassicalTurtleDonchianStrategy(CtaTemplate):
    """经典海龟：唐奇安通道策略
    - 两个唐奇安通道指标，20-10和 55-20，分别称之为小周期和大周期
    - 当突破小周期上轨做多，突破下轨做空
    - 分仓入场，最后入场的仓位价格下跌 2atr 之后止损平仓
    - 当反转触及止盈周期时，止盈离场
    - 当前一次离场是止盈离场，则进入等待状态，下一次小周期入场信号忽略
    - 当处于等待状态时，触发了大周期入场信号，依然入场
    """
    className = 'ClassicalTurtleDonchianStrategy'
    name = '经典海龟：唐奇安通道策略'
    author = 'lamter'

    # 策略参数
    DOWN_IN = UP_IN = 20  # 小周期入场
    DOWN_OUT = UP_OUT = 10  # 小周期离场
    BIG_DOWN_IN = BIG_UP_IN = 55  # 大周期入场
    BIG_DOWN_OUT = BIG_UP_OUT = 20  # 大周期离场
    ATR_N = 20  # ATR 长度
    BIG_ATR_N = 55   # 大周期 ATR 长度
    fixhands = 1  # 固定手数
    UNITS = 4  # 分仓数量
    ADD_ATR = 0.5  # 每 0.5 ATR 加仓一次
    STOP_ATR = 2  # 止损ATR
    BIG = True  # 是否启用大周期

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'DOWN_IN', 'UP_IN', 'DOWN_OUT', 'UP_OUT', 'BIG_DOWN_IN', 'BIG_UP_IN', 'BIG_UP_OUT', 'BIG_DOWN_OUT',
        'ATR_N','BIG_ATR_N',
        'UNITS',
        'BIG',
        'fixhands',
    ])

    # 策略变量
    upIn = None  # 多点入场
    downIn = None  # 空入场
    upOut = None  # 多离场
    downOut = None  # 低点离场
    bigUpIn = None  # 大周期多点入场
    bigDownIn = None  # 空入场
    bigUpOut = None  # 多离场
    bigDownOut = None  # 低点离场
    atr = None  # ATR 值
    bigAtr = None  # 大周期 ATR 值
    big = False  # 是否处于大周期，开启默认处于小周期
    smallLongInList = []  # 大周期中的小周期开仓价
    smallShortInList = []  # 大周期中的小周期开仓价
    smallUnits = 0  # 大周期时记录小周期的仓位
    smallAtr = None  # 大周期时的小周期ATR

    # 变量列表，保存了变量的名称
    _varList = [
        'upIn',
        'downIn',
        'upOut',
        'downOut',
        'bigUpIn',
        'bigDownIn',
        'bigUpOut',
        'bigDownOut',
        'atr',
        'bigAtr',
        'big',
        'smallLongInList',
        'smallShortInList',
        'smallUnits',
        'smallAtr',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(ClassicalTurtleDonchianStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands or 1
        self.techIndLine = {
            'upIn': ([], []), 'downIn': ([], []),
            'upOut': ([], []), 'downOut': ([], []),
            'bigUpIn': ([], []), 'bigDownIn': ([], []),
            'bigUpOut': ([], []), 'bigDownOut': ([], [])}

        self.units = [Unit(i, self) for i in range(0, self.UNITS)]
        self.vtOrderID2Unit = {}  # {'vtOrderID': Unit}

    @property
    def direction(self):
        if self.pos > 0:
            return DIRECTION_LONG
        if self.pos < 0:
            return DIRECTION_SHORT
        if self.pos == 0:
            return None

    @property
    def isBig(self):
        return self.big and self.BIG

    def initMaxBarNum(self):
        barNum = 0
        for p in self.paramList:
            if '_' in p:
                barNum = max(barNum, getattr(self, p))

        self.maxBarNum = barNum * 2

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog('%s策略初始化' % self.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.maxBarNum)

        self.log.info('即将加载 {} 条 bar 数据'.format(len(initData)))

        self.initContract()

        # 从数据库加载策略数据，要在加载 bar 之前。因为数据库中缓存了技术指标
        if not self.isBackTesting():
            # 需要等待保证金加载完毕
            document = self.fromDB()
            self.loadCtaDB(document)

        for bar in initData:
            self.bm.bar = bar
            if not self.isBackTesting():
                self.tradingDay = bar.tradingDay
            self.onBar(bar)
            print(self.bar.datetime)
            self.bm.preBar = bar

        # self.log.warning(u'加载的最后一个 bar {}'.format(bar.datetime))
        if len(initData) >= self.maxBarNum * self.barXmin:
            self.log.info('初始化完成')
        else:
            self.log.warning('初始化数据不足!')

        # self.updateHands()

        self.isCloseoutVaild = True
        self.putEvent()

    # ----------------------------------------------------------------------
    @exception
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.log.info('%s策略启动' % self.name)

        if not self.isBackTesting():
            # 实盘，可以存库。
            self.saving = True

        if self.bar is None:
            return
        
        # 更新技术指标
        self.updateHands()
        self.updateUnitInd()

        # 撤所有的单
        self.cancelAll()

        # 开盘前再下单
        self.orderUntilTradingTime()

        self.putEvent()

    def _orderOnThreading(self):
        """
        开盘前5秒下单
        :return:
        """
        # 下单
        self.orderOpenOnStart()
        self.orderCloseOnStart()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.saveDB()
        self.log.info('%s策略停止' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        if self.trading:
            self.bm.updateTick(tick)

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """
        self.bar 更新完最后一个 tick ，在生成新的 bar 之前将 self.bar 传入
        该函数是由下一根 bar 的第一个 tick 驱动的，而不是当前 bar 的最后一个 tick
        :param bar:
        :return:
        """
        # 此处先调用 self.onXminBar
        self.bm.updateXminBar(bar)

        if not self.trading:
            return

        if self.isCloseoutVaild and self.rtBalance < 0:
            # 爆仓，一键平仓
            self.closeout()

        if self.isBig and self.pos == 0:
            # 处于大周期，且尚未开仓
            self.simSmallTradeOnBar(bar)

    def simSmallTradeOnBar(self, bar):
        """
        处于大周期时，模拟小周期开仓
        :return:
        """
        # 设置模拟开仓
        if self.smallUnits >= 0:
            longInUnits = 0
            for smallLongIn in self.smallLongInList:
                if bar.high >= smallLongIn:
                    if not self.smallAtr:
                        self.smallAtr = self.atr
                    longInUnits += 1
            self.smallUnits = max(longInUnits, self.smallUnits)
        elif self.smallUnits <= 0:
            shortInUnits = 0
            for smallShortIn in self.smallShortInList:
                if bar.low <= smallShortIn:
                    if not self.smallAtr:
                        self.smallAtr = self.atr
                    shortInUnits -= 1
            self.smallUnits = min(shortInUnits, self.smallUnits)

        # 模拟平仓
        if self.smallUnits > 0:
            longIn = self.smallLongInList[abs(self.smallUnits) - 1]
            stopPrice = longIn - self.STOP_ATR * self.smallAtr
            longOut = max(self.downOut, stopPrice)
            # 触发小周期平仓
            setSmall = bar.low <= longOut

        elif self.smallUnits < 0:
            shortIn = self.smallShortInList[abs(self.smallUnits) - 1]
            stopPrice = shortIn + self.STOP_ATR * self.smallAtr
            shortOut = min(self.upOut, stopPrice)
            setSmall = bar.high >= shortOut
        else:
            setSmall = False

        if setSmall:
            # 触发小周期平仓
            self.log.info('小周期平仓')
            self.setSmall()

    def orderOpenOnStart(self):
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                # 空仓状态才下开仓单
                self.unitOrderOpen(u)

    def orderOpenOnXminBar(self):
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                # 空仓状态才下开仓单
                self.unitOrderOpen(u)

    def orderOpenOnSetSmall(self):
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                # 空仓状态才下开仓单
                self.unitOrderOpen(u)

    def orderOpenOnTrad(self):
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                # 空仓状态才下开仓单
                self.unitOrderOpen(u)

    def unitOrderOpen(self, unit):
        """

        :return:
        """
        assert isinstance(unit, Unit)
        if unit.pos != 0:
            # 该仓位已经开仓了
            return

        if self.direction in (DIRECTION_LONG, None):
            # 开多
            if unit.longInSO and unit.longInSO.price == unit.longIn:
                # 已经有开多单，且价格没变化
                # 则不需要再重新下单
                pass
            elif unit.longIn is None:
                # 技术指标尚未准备好
                pass
            else:
                if unit.longInSO:
                    self.log.info('撤单重发 {} -> {}'.format(unit.longInSO.price, unit.longIn))
                    self.cancelOrder(unit.longInSO.stopOrderID)
                stopOrderID = self.buy(unit.longIn, self.hands, stop=True)[0]
                # 互相绑定停止单
                so = self.ctaEngine.workingStopOrderDict[stopOrderID]
                so.unit = unit
                unit.longInSO = so
                self.log.info('开多单 {} {}'.format(unit, so))

        if self.direction in (DIRECTION_SHORT, None):
            # 开多
            if unit.shortInSO and unit.shortInSO.price == unit.shortIn:
                # 已经有开空单，且价格没变化
                # 则不需要再重新下单
                pass
            elif unit.shortIn is None:
                # 技术指标尚未准备好
                pass
            else:
                if unit.shortInSO:
                    self.log.info('撤单重发 {} -> {}'.format(unit.shortInSO.price, unit.shortIn))
                    self.cancelOrder(unit.shortInSO.stopOrderID)
                stopOrderID = self.short(unit.shortIn, self.hands, stop=True)[0]
                # 互相绑定停止单
                so = self.ctaEngine.workingStopOrderDict[stopOrderID]
                so.unit = unit
                unit.shortInSO = so
                self.log.info('开空单 {} {}'.format(unit, so))

    def orderCloseOnTrade(self):
        """
        下平仓单
        :return:
        """
        for u in self.units:
            self.unitOrderClose(u)

    def orderCloseOnXminBar(self):
        for u in self.units:
            self.unitOrderClose(u)

    def orderCloseOnStart(self):
        for u in self.units:
            self.unitOrderClose(u)

    def unitOrderClose(self, unit):
        """

        :param unit:
        :return:
        """
        if unit.pos == 0:
            return

        unitOpenCount = sum([1 for u in self.units if u.status != u.STATUS_EMPTY]) - 1
        if unit.pos > 0:
            # 平多
            # 计算止损价
            stopPrice = self.roundToPriceTick(
                unit.openPrice - (self.STOP_ATR - (unitOpenCount - unit.index) * self.ADD_ATR) * unit.atr)
            # 对比止盈价，
            longOut = max(stopPrice, unit.longOut)
            if unit.longOutSO and unit.longOutSO.price == longOut:
                pass
            else:
                if unit.longOutSO:
                    self.log.info('撤单重发 {} -> {}'.format(unit.longOutSO.price, longOut))
                    self.cancelOrder(unit.longOutSO.stopOrderID)
                stopOrderID = self.sell(longOut, abs(unit.pos), stop=True)[0]
                so = self.ctaEngine.workingStopOrderDict[stopOrderID]
                so.unit = unit
                unit.longOutSO = so
                self.log.info('空平单 {} {}'.format(unit, so))

        elif unit.pos < 0:
            # 平空
            stopPrice = self.roundToPriceTick(
                unit.openPrice + (self.STOP_ATR - (unitOpenCount - unit.index) * self.ADD_ATR) * unit.atr)
            # 对比止盈价
            shortOut = min(stopPrice, unit.shortOut)
            if unit.shortOutSO and unit.shortOutSO.price == shortOut:
                pass
            else:
                if unit.shortOutSO:
                    self.log.info('撤单重发 {} -> {}'.format(unit.shortOutSO.price, shortOut))
                    self.cancelOrder(unit.shortOutSO.stopOrderID)
                stopOrderID = self.cover(shortOut, abs(unit.pos), stop=True)[0]
                so = self.ctaEngine.workingStopOrderDict[stopOrderID]
                so.unit = unit
                unit.shortOutSO = so
                self.log.info('多平单 {} {}'.format(unit, so))

    # ----------------------------------------------------------------------
    def onXminBar(self, xminBar):
        """
        这个函数是由 self.xminBar 的最后一根 bar 驱动的
        执行完这个函数之后，会立即更新到下一个函数
        :param xminBar:
        :return:
        """
        bar = xminBar

        # 保存K线数据
        am = self.am

        am.updateBar(bar)

        # 计算唐奇安指标
        self.upIn, self.downIn = self.getUpDown(self.UP_IN, self.DOWN_IN)
        self.downOut, self.upOut = self.getUpDown(self.DOWN_OUT, self.UP_OUT)
        self.bigUpIn, self.bigDownIn = self.getUpDown(self.BIG_UP_IN, self.BIG_DOWN_IN)
        self.bigDownOut, self.bigUpOut = self.getUpDown(self.BIG_DOWN_OUT, self.BIG_UP_OUT)

        self.atr = self.roundToPriceTick(am.atr(self.ATR_N))
        self.bigAtr = self.roundToPriceTick(am.atr(self.BIG_ATR_N))

        # msg = u''
        # for k in self._varList:
        #     v = getattr(self, k)
        #     msg += u'{}:{}\t'.format(k, v)
        # self.log.info(msg)

        if not am.inited:
            return

        self.saveTechIndOnXminBar(bar.datetime)

        if self.trading:
            # 当没有持仓的时候，更改开仓价格
            self.updateUnitInd()
            # 开仓单
            self.orderOpenOnXminBar()
            # 平仓单
            self.orderCloseOnXminBar()

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    def updateUnitInd(self):
        """
        更新每个 Units 的指标
        :return:
        """
        for k in self._varList:
            if k.startswith('small'):
                continue
            v = getattr(self, k)
            if v == None:
                self.log.info('技术指标尚未准备好 {} {}'.format(k, v))
                return

        if self.smallUnits == 0:
            self.smallLongInList = [self.roundToPriceTick(self.upIn + i * self.ADD_ATR) for i in range(self.UNITS)]
            self.smallShortInList = [self.roundToPriceTick(self.downIn - i * self.ADD_ATR) for i in range(self.UNITS)]

        if self.pos == 0:
            # 更新入场指标
            # 空仓时才更新，一旦开仓，所有入场指标都固定
            for u in self.units:
                u.atr = self.roundToPriceTick(self.bigAtr if self.isBig else self.atr)
                longIn, shortIn = (self.bigUpIn, self.bigDownIn) if self.isBig else (self.upIn, self.downIn)
                # 直接根据公式 轨道 ± n * ATR 来计算开仓价格
                # 当 pos != 0，即已经开仓后，其他的 Unit 的开仓价格不再变化
                u.longIn = self.roundToPriceTick(longIn + (u.index * self.ADD_ATR) * u.atr)
                u.shortIn = self.roundToPriceTick(shortIn - (u.index * self.ADD_ATR) * u.atr)
                # self.log.warning(u'{}'.format(u))
                u.hands = self.hands

        # 退出指标，任何时候都可以更新退出指标
        longOut, shortOut = (self.bigUpOut, self.bigDownOut) if self.isBig else (self.upOut, self.downOut)
        for u in self.units:
            u.longOut = self.roundToPriceTick(longOut)
            u.shortOut = self.roundToPriceTick(shortOut)

    def getUpDown(self, up, down):
        """
        小周期入场
        :param period:
        :return:
        """
        am = self.am
        if up == down:
            return am.donchian(up)
        else:
            longin, _ = am.donchian(up)
            _, shortin = am.donchian(down)
            return longin, shortin

    def saveTechIndOnXminBar(self, dt):
        """
        保存技术指标
        :return:
        """
        for indName, [dtList, dataList] in list(self.techIndLine.items()):
            data = getattr(self, indName)
            dtList.append(dt)
            dataList.append(self.roundToPriceTick(data))

    def updateHands(self):
        """
        更新开仓手数
        :return:
        """

        if self.capital <= 0:
            self.hands = 0
            return

        # 以下技术指标为0时，不更新手数
        # 在长时间封跌涨停板后，会出现以下技术指标为0的情况
        if self.atr == 0:
            self.hands = 0
            return

        # 固定仓位
        if self.fixhands is not None:
            # 有固定手数时直接使用固定手数
            self.hands = min(self.maxHands, self.fixhands)
            return

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """
        响应停止单
        :param so:
        :return:
        """
        # for u in self.units:
        #     self.log.info(u.stopOrderToLog())

        assert isinstance(so, StopOrder)
        if so.status == STOPORDER_CANCELLED:
            # 撤单
            unit = so.unit
            assert isinstance(unit, Unit)
            unit.dropSO(so)
            self.log.info('撤单 {} {} {}'.format(unit, so, so.vtOrderID))
        elif so.status == STOPORDER_TRIGGERED:
            # 触发，此时 Unit 状态只有 EMPTY or FULL
            # 因为在其他状态不会有停止单可以触发
            unit = so.unit
            assert isinstance(unit, Unit)
            unit.dropSO(so)

            self.vtOrderID2Unit[so.vtOrderID] = unit
            self.log.info('绑定 {} {}'.format(so.vtOrderID, unit))

            if unit.status == unit.STATUS_EMPTY:
                # 开仓单
                self.log.info(unit.setStatus(unit.STATUS_OPENING))
                  # 更改状态
                # 撤销所有反方向开仓单
                for u in self.units:
                    if so.direction == DIRECTION_LONG:
                        if u.shortInSO:
                            self.cancelOrder(u.shortInSO.stopOrderID)
                    elif so.direction == DIRECTION_SHORT:
                        if u.longInSO:
                            self.cancelOrder(u.longInSO.stopOrderID)
                    else:
                        self.log.error('未知的停止单方向 {}'.format(so))
            elif unit.status == unit.STATUS_FULL:
                # 平仓单
                self.log.info(unit.setStatus(unit.STATUS_DONE))
            else:
                self.log.warning('异常的 Unit.status {} {}'.format(unit, so))
        else:  # so.status == STOPORDER_WAITING:
            # 刚挂单，没有需要处理的
            pass

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        log = self.log.info
        if order.status == STATUS_REJECTED:
            log = self.log.warning
            message = ''
            for k, v in list(order.rawData.items()):
                message += '{}:{}\n'.format(k, v)
            log(message)
            unit = self.vtOrderID2Unit.pop(order.vtOrderID)
        elif order.status == STATUS_CANCELLED:
            unit = self.vtOrderID2Unit.pop(order.vtOrderID)

        log('状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))
        # self.log.warning(u'{vtOrderID} 状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        originCapital, charge, profile = self._onTrade(trade)

        try:
            self.log.info('{}'.format(trade.vtOrderID))
            unit = self.vtOrderID2Unit[trade.vtOrderID]
            self.log.info('{}'.format(unit))
        except KeyError:
            for vtOrderID, u in list(self.vtOrderID2Unit.items()):
                self.log.error(vtOrderID)
                self.log.error(str(u))
            raise
        assert isinstance(unit, Unit)

        posChange = self.pos - self.prePos

        # 开平仓成本
        if unit.status == unit.STATUS_OPENING:
            unit.openTurnover += trade.volume * trade.price
        # 平仓成本
        if unit.status == unit.STATUS_DONE:
            unit.closeTurnover += trade.volume * trade.price

        # 统计仓位
        unit.pos += posChange

        # 更改状态
        if abs(unit.pos) == unit.hands and unit.hands > 0:
            self.log.info(unit.setStatus(unit.STATUS_FULL))

        if self.pos == 0 and Unit.STATUS_DONE in [u.status for u in self.units]:
            # 仓位平仓完成,且有仓位开仓了
            # 本次入场结束，统计盈利，重置
            # 统计盈利
            self.log.info('全部平仓完成'.format())
            profile = 0
            for u in self.units:
                if posChange > 0:
                    profile += u.closeTurnover - u.openTurnover
                else:
                    profile += u.openTurnover - u.closeTurnover

            if profile > 0:
                self.setBig()
            else:
                self.setSmall()
            # 盈利，转为使用大周期

            # 重置
            self.cancelAll()
            for u in self.units:
                u.clear()

            # 重新下开仓单
            self.updateHands()
            self.updateUnitInd()
            self.orderOpenOnTrad()

        # 下平仓单
        self.orderCloseOnTrade()

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

        self.log.info(self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile))

    def setBig(self):
        self.log.info('进入大周期')
        self.big = True

    def setSmall(self):
        self.log.info('进入小周期')
        self.big = False
        self.smallLongInList = []  # 大周期中的小周期开仓价
        self.smallShortInList = []  # 大周期中的小周期开仓价
        self.smallUnits = 0  # 大周期时记录小周期的仓位
        self.smallAtr = None  # 大周期时的小周期ATR

        # 重新下开仓单
        self.updateUnitInd()
        self.orderOpenOnSetSmall()

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(ClassicalTurtleDonchianStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        dic['units'] = [u.toSave() for u in self.units]
        return dic

    def loadCtaDB(self, document=None):
        super(ClassicalTurtleDonchianStrategy, self).loadCtaDB(document)
        if document and 'units' in document:
            units = document.pop('units')
            for i, dic in enumerate(units):
                try:
                    unit = self.units[i]
                    unit.fromDB(dic)
                except IndexError:
                    # 存库时是4仓，现在只有3仓
                    self.log.error('存库中有 {} 个 Unit 目前策略只有 {} 个Unit'.format(len(units), len(self.units)))
                    break

        self._loadVar(document)

    def toHtml(self):
        orderDic = super(ClassicalTurtleDonchianStrategy, self).toHtml()
        orderDic['units'] = pd.DataFrame([u.toHtml() for u in self.units]).to_html()
        return orderDic


class Unit(object):
    """
    经典海龟的仓位
    """

    STATUS_EMPTY = '空仓'
    STATUS_OPENING = '开仓中'
    STATUS_FULL = '满仓'
    STATUS_DONE = '完结'

    def __init__(self, index, strategy):
        assert isinstance(strategy, ClassicalTurtleDonchianStrategy)
        self.strategy = strategy
        self.index = index
        self.pos = 0  # 多正空负
        self.status = self.STATUS_EMPTY

        self.hands = 0  # 开仓手数
        self.atr = None
        self.longIn = None
        self.shortIn = None
        self.longOut = None
        self.shortOut = None

        self.longInSO = None  # 开多停止单
        self.shortInSO = None  # 开空停止单
        self.longOutSO = None  # 平多停止单
        self.shortOutSO = None  # 平空停止单

        self.openTurnover = 0  # 开仓成本
        self.closeTurnover = 0  # 平仓成本

    def __str__(self):
        s = '<Unit:{}'.format(self.index)
        # 'openTurnover', 'closeTurnover'
        for k in ['status', 'pos', 'atr', 'longIn', 'shortIn', 'longOut', 'shortOut', ]:
            v = getattr(self, k)
            if isinstance(v, float):
                v = self.strategy.roundToPriceTick(v)
            s += '\t{}:{}'.format(k, v)
        return s + '>'

    def toSave(self):
        dic = {}
        for k, v in list(self.__dict__.items()):
            try:
                if v.__dict__:
                    continue
            except AttributeError:
                dic[k] = v
        return dic

    def fromDB(self, dic):
        for k, v in list(dic.items()):
            setattr(self, k, v)

    def clear(self):
        self.strategy.log.info(self.setStatus(self.STATUS_EMPTY))

        self.hands = 0  # 开仓手数
        self.atr = None
        self.longIn = None
        self.shortIn = None
        self.longOut = None
        self.shortOut = None

        self.longInSO = None  # 开多停止单
        self.shortInSO = None  # 开空停止单

        self.openTurnover = 0  # 开仓成本
        self.closeTurnover = 0  # 平仓成本

    @property
    def openPrice(self):
        return abs(self.openTurnover / self.pos)

    def setStatus(self, status):
        log = 'Unit:{} {} -> {}'.format(self.index, self.status, status)
        self.status = status
        return log

    def getAllStopOrders(self):
        stopOrderIDs = []
        for k, v in list(self.__dict__.items()):
            if isinstance(v, StopOrder):
                stopOrderIDs.append(v)
        return stopOrderIDs

    def stopOrderToLog(self):
        _all = self.getAllStopOrders()
        log = '{} 个 so\n'.format(len(_all))
        log += '\n'.join([str(so) for so in _all])
        return log

    def dropSO(self, so):
        for k, v in list(self.__dict__.items()):
            # 撤单后要重置停止单
            if v == so:
                setattr(self, k, None)
                break
        else:
            self.strategy.log.warning('未绑定停止单 {} {}'.format(self, so))
            self.strategy.log.warning(self.stopOrderToLog())

    def toHtml(self):
        dic = self.toSave()
        return dic
