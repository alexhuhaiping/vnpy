# encoding: UTF-8




from threading import Timer
from collections import OrderedDict
import time

import arrow

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
    LOW_IN = HIGH_IN = 20  # 小周期入场
    LOW_OUT = HIGH_OUT = 10  # 小周期离场
    BIG_LOW_IN = BIG_HIGH_IN = 55  # 大周期入场
    BIG_LOW_OUT = BIG_HIGH_OUT = 20  # 大周期离场
    ATR_N = 20  # ATR 长度
    fixhands = 1  # 固定手数
    UNITS = 4  # 分仓数量
    ADD_ATR = 0.5  # 每 0.5 ATR 加仓一次
    STOP_ATR = 2  # 止损ATR
    BIG = True  # 是否启用大周期

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'LOW_IN', 'HIGH_IN', 'LOW_OUT', 'HIGH_OUT', 'BIG_LOW_IN', 'BIG_HIGH_IN', 'BIG_HIGH_OUT', 'BIG_LOW_OUT',
        'ATR_N',
        'UNITS',
        'BIG',
    ])

    # 策略变量
    longIn = None  # 多点入场
    shortIn = None  # 空入场
    longOut = None  # 多离场
    shortOut = None  # 低点离场
    bigLongIn = None  # 大周期多点入场
    bigShorIn = None  # 空入场
    bigLongOut = None  # 多离场
    bigShortOut = None  # 低点离场
    atr = 0  # ATR 值
    status = None  # 是否处于等待阶段

    # 变量列表，保存了变量的名称
    _varList = [
        'longIn',
        'shortIn',
        'longOut',
        'shortOut',
        'bigLongIn',
        'bigShortIn',
        'bigLongOut',
        'bigShortOut',
        'atr',
        'status',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    STATUS_SMALL = '小周期'
    STATUS_BIG = '大周期'

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(ClassicalTurtleDonchianStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands or 1
        self.techIndLine = {
            'longIn': ([], []), 'shortIn': ([], []),
            'longOut': ([], []), 'shortOut': ([], []),
            'bigLongIn': ([], []), 'bigShortIn': ([], []),
            'bigLongOut': ([], []), 'bigShortOut': ([], [])}

        self.status = self.STATUS_SMALL
        self.units = [Unit(i, self) for i in range(0, self.UNITS)]

        self.stopPrice = None  # 统一的平仓价格

        self.stopOrderID2Unit = {}  # {'orderID': Unit()} orderID 可以是 vtOrderID, stopOrderID, orderID
        self.orderID2Unit = {}  # {'orderID': Unit()} orderID 可以是 vtOrderID, stopOrderID, orderID

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
            self.bm.preBar = bar

        # self.log.warning(u'加载的最后一个 bar {}'.format(bar.datetime))
        if len(initData) >= self.maxBarNum * self.barXmin:
            self.log.info('初始化完成')
        else:
            self.log.warning('初始化数据不足!')

        self.updateHands()

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

        self.updateUnitInd()

        # 开盘再下单
        self.orderUntilTradingTime()

        self.putEvent()

    def _orderOnThreading(self):
        """
        开盘后下单的调用
        :return:
        """
        # 开仓单和平仓单
        self.cancelAll()
        self.orderOnStart()

    def orderOnStart(self):
        """
        在策略启动时下单
        :return:
        """
        self.cancelAll()

        # 基于小周期
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                u.orderOpenOnStart()
                # todo 满仓中，下平仓单

    def orderOpenOnTrade(self):
        for u in self.units:
            if u.status == u.STATUS_EMPTY:
                u.orderOpenOnTrade()

    @property
    def direction(self):
        if self.pos > 0:
            return DIRECTION_LONG
        if self.pos < 0:
            return DIRECTION_SHORT
        if self.pos == 0:
            return None

    def orderOnBar(self):
        """
        在策略启动时下单
        :return:
        """
        for u in self.units:
            # todo 重新下开仓单
            u.orderOpenOnBar()
            # todo 重新下平仓单

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

        if self.isCloseoutVaild and self.rtBalance < 0:
            # 爆仓，一键平仓
            self.closeout()

        if self.trading:
            self.orderOnBar()

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

        if not am.inited:
            return

        # 计算唐奇安指标
        self.longIn, self.shortIn = self.getUpDown(self.HIGH_IN, self.LOW_IN)
        self.shortOut, self.longOut = self.getUpDown(self.LOW_OUT, self.HIGH_OUT)
        self.bigLongIn, self.bigShortIn = self.getUpDown(self.BIG_HIGH_IN, self.BIG_LOW_IN)
        self.bigShortOut, self.bigLongOut = self.getUpDown(self.BIG_LOW_OUT, self.BIG_HIGH_OUT)

        self.atr = am.atr(self.ATR_N)

        self.saveTechIndOnXminBar(bar.datetime)

        if self.trading:
            self.updateHands()
            # 当没有持仓的时候，更改开仓价格
            self.updateUnitInd()

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    def updateUnitInd(self):
        """
        更新每个 Units 的指标
        :return:
        """
        if self.pos == 0:
            for u in self.units:
                u.atr = self.atr
                longIn, shortIn = (self.longIn, self.shortIn) if self.isSmall() else (self.bigLongIn, self.bigShortIn)
                # 直接根据公式 轨道 ± n * ATR 来计算开仓价格
                # 当 pos != 0，即已经开仓后，其他的 Unit 的开仓价格不再变化
                u.longIn = longIn + (u.index * self.ADD_ATR) * self.atr
                u.shortIn = shortIn - (u.index * self.ADD_ATR) * self.atr
                # self.log.warning(u'{}'.format(u))
                u.hands = self.hands
        # 退出点
        for u in self.units:
            u.longOut, u.shorOut = (self.longOut, self.shortOut) if self.isSmall() else (
                self.bigLongOut, self.bigShortOut)

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
            dataList.append(data)

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

            # 补发
            self.orderUntilTradingTime()

        log('状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        originCapital, charge, profile = self._onTrade(trade)

        unit = self.orderID2Unit.get(trade.vtOrderID)
        self.log.info('{}'.format(unit))

        assert isinstance(unit, Unit)

        posChange = self.pos - self.prePos

        # 只要成交了，必然要检查撤掉反方向的开仓单
        for u in self.units:
            _ids = [_id for _id in u.openStopOrder]
            for stopOrderID in _ids:
                self.cancelOrder(stopOrderID)

        # 状态判定
        self.setStatusOnTrade(unit, posChange, trade)

        # 根据状态做处理
        self.doByStatusOnTrade(unit, posChange)

        # 是否全部完成了平仓：全部为空
        isCloseAll = False not in [u.isEmpty() for u in self.units]

        if isCloseAll:
            # 全部平仓完成
            self.log.info('全部平仓完成，开始重置')
            profile = sum([u.profile for u in self.units if u.profile])
            if profile > 0:
                # 上次盈利了，进入大周期
                self.setStatus(self.STATUS_BIG)
            else:
                # 没盈利，进入小周期
                self.setStatus(self.STATUS_SMALL)

            for u in self.units:
                # 重置，可以重新开仓
                u.profile = None
                u.direction = None
                u.setStatus(unit.STATUS_EMPTY)

            self.updateUnitInd()

        self.orderOpenOnTrade()

        self.log.info('检查成交位置')
        for u in self.units:
            self.log.info('{}'.format(u))
            for openSo in list(u.openStopOrder.values()):
                self.log.info('{}'.format(openSo))

        # if arrow.get('2018-11-30 11:00+08').datetime <= self.bar.datetime <= arrow.get(
        #         '2018-11-30 13:59:00+08').datetime:

        # time.sleep(0.2)
        self.log.info(self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile))

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    def doByStatusOnTrade(self, unit, posChange):
        """
        状态机
        :return:
        """
        if unit.status == unit.STATUS_DONE:
            # 平仓完毕
            # 计算盈亏
            if posChange < 0:
                # 平多
                unit.profile = unit.closeTurnover - unit.openTurnover
            else:
                unit.profile = unit.openTurnover - unit.closeTurnover

            # 完结，重置之前不再开仓
            unit.hands = None
            unit.closeTurnover = unit.openTurnover = None
            unit.longIn = unit.shortIn = None

        elif unit.status == unit.STATUS_OPENING:
            # 开仓中
            # 下平仓单
            self.orderCloseOnTrade()
        elif unit.status == unit.STATUS_FULL:
            # 满仓，下平仓单
            self.orderCloseOnTrade()

    def setStatusOnTrade(self, unit, posChange, trade):
        """
        在 onTrade 设置状态
        :return:
        """
        if unit.isEmpty():
            # 开仓
            # 计算持仓成本
            unit.openTurnover = trade.price * trade.volume
            # 仓位操作
            unit.pos += posChange
            if unit.isFull():
                # 满仓
                unit.setStatus(unit.STATUS_FULL)
            else:
                # 还没满仓
                unit.setStatus(unit.STATUS_OPENING)

        elif unit.isFull():
            # 已经处于满仓状态，开始进行平仓操作
            # 平仓成本
            unit.closeTurnover = trade.price * trade.volume
            # 仓位操作
            unit.pos += posChange

            # 平仓完成、未完成都设为完结状态
            unit.setStatus(unit.STATUS_DONE)

        else:  # unit.isTrading()
            if self.pos > 0 and posChange > 0:
                # 加仓
                unit.openTurnover += trade.price * trade.volume
            else:
                # 减仓
                unit.closeTurnover += trade.price * trade.volume

            # 仓位操作
            unit.pos += posChange
            # 开仓时
            if unit.isFull():
                unit.setStatus(unit.STATUS_FULL)
            # 平仓时
            if unit.isEmpty():
                unit.setStatus(unit.STATUS_DONE)

    def orderCloseOnTrade(self):
        """
        指定单元挂平仓单
        :param unit:
        :return:
        """
        self.log.info('挂平仓单')

        if self.direction == None:
            raise ValueError('此处 direction != None')

        # 平仓价格
        for u in self.units[::-1]:
            if u.pos != 0:
                break
        else:
            raise ValueError('此处不可能所有 unit.pos 均为 0 ')

        if self.direction == DIRECTION_LONG:
            price = u.longIn - u.atr * self.STOP_ATR
        else:  # self.direction == DIRECTION_SHORT:
            price = u.shortIn + u.atr * self.STOP_ATR

        # 下平仓单
        for u in self.units:
            if u.pos != 0:
                self.log.info('{}'.format(u))
                for _id, so in list(u.closeStopOrder.items()):
                    # 撤平仓单
                    self.cancelOrder(_id)
                    self.log.info('{}'.format(so))
                for _id, so in list(u.openStopOrder.items()):
                    # 撤开仓单
                    self.cancelOrder(_id)
                    self.log.info('{}'.format(so))

                if self.direction == DIRECTION_LONG:
                    vtOrderIDs = self.sell(price, abs(u.pos), stop=True)
                else:  # self.direction == DIRECTION_SHORT:
                    vtOrderIDs = self.cover(price, abs(u.pos), stop=True)

                for _id in vtOrderIDs:
                    so = self.ctaEngine.workingStopOrderDict[_id]
                    self.stopOrderID2Unit[so.stopOrderID] = u
                    u.closeStopOrder[so.stopOrderID] = so
                    self.log.info('{}'.format(so))

    def setStatus(self, status):
        if status == self.STATUS_BIG:
            self.status = self.STATUS_BIG if self.BIG else self.STATUS_SMALL
        else:
            self.status = status

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
        self._loadVar(document)

    def isBig(self):
        return self.status == self.STATUS_BIG

    def isSmall(self):
        return self.status == self.STATUS_SMALL

    def onStopOrder(self, so):
        """
        经过这个函数之后，才返回 vtOrderIDs
        :param so:
        :return:
        """
        assert isinstance(so, StopOrder)

        if so.status == STOPORDER_CANCELLED:
            # 撤单
            unit = self.stopOrderID2Unit.pop(so.stopOrderID)
            # self.log.info(u'{} {} {}'.format(so, unit, so.vtOrderID))
            try:
                unit.openStopOrder.pop(so.stopOrderID)
            except KeyError:
                unit.closeStopOrder.pop(so.stopOrderID)
        elif so.status == STOPORDER_TRIGGERED:
            # 触发，从缓存中丢弃
            unit = self.stopOrderID2Unit.pop(so.stopOrderID)
            self.orderID2Unit[so.vtOrderID] = unit
            # self.log.info(u'orderID2Unit: {} {}'.format(so.vtOrderID, unit))
            # self.log.info(u'{} {} {}'.format(so, unit, so))
            try:
                unit.openStopOrder.pop(so.stopOrderID)
            except KeyError:
                unit.closeStopOrder.pop(so.stopOrderID)

        else:  # so.status == STOPORDER_WAITING:
            # 挂单
            pass

    @property
    def firstUnit(self):
        """
        第一仓对象
        :return:
        """
        return self.units[0]


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
        # self.openPrice = None  # 技术开仓价

        self.atr = None  # 开仓时的 ATR
        self.openStopOrder = {}  # {'stopOrderID': StopOrder()}
        self.closeStopOrder = {}  # {'stopOrderID': StopOrder()}
        self.hands = None
        self.openTurnover = 0  # 持仓成本
        self.closeTurnover = 0  # 平仓成本

        self.profile = None  # 上次盈利
        self.longIn = None  # 技术点位
        self.longOut = None  # 技术点位
        self.shortIn = None  # 技术点位
        self.shortOut = None  # 技术点位

    def cancelOpenStopOrder(self):
        """
        取消停止单
        :return:
        """
        for stopOrderID in self.openStopOrder:
            self.strategy.cancelOrder(stopOrderID)

    @property
    def isFirst(self):
        return self.strategy.firstUnit == self

    def toSave(self):
        """

        :return:
        """
        return {
            'pos': self.pos
        }

    def loadCtaDB(self, dic):
        for k, v in list(dic.items()):
            setattr(self, k, v)

    def isEmpty(self):
        """
        空仓中
        :return:
        """
        return self.pos == 0

    def isFull(self):
        return self.hands == abs(self.pos)

    def isTrading(self):
        return not self.isEmpty() and not self.isFull()

    def __str__(self):
        s = '<Unit:{}'.format(self.index)
        for k in ['status', 'pos', 'atr', 'longIn', 'shortIn', 'longOut', 'shortOut', 'openTurnover', 'closeTurnover']:
            v = getattr(self, k)
            if isinstance(v, float):
                v = self.strategy.roundToPriceTick(v)
            s += '\t{}:{}'.format(k, v)
        return s + '>'

    def orderOpenOnStart(self):
        """
        启动时下开仓单
        :return:
        """
        self._orderOpen()

    def orderOpenOnTrade(self):
        self._orderOpen()

    def orderOpenOnBar(self):
        self._orderOpen()

    def _orderOpen(self):
        s = self.strategy
        # 空仓状态
        if self.isEmpty():
            vtOrderIDs = []
            longSo = shortSo = None
            for so in list(self.openStopOrder.values()):
                if so.direction == DIRECTION_LONG:
                    longSo = so
                elif so.direction == DIRECTION_SHORT:
                    shortSo = so

            if s.direction in (DIRECTION_LONG, None):
                # 开多仓
                if longSo and longSo.price == self.longIn and longSo.volume == s.hands:
                    # 已经有多单了，不需要再下多单
                    pass
                else:
                    if longSo:
                        s.log.info('撤开多单重新下')
                        s.cancelOrder(longSo.stopOrderID)
                    vtOrderIDs.extend(s.buy(self.longIn, s.hands, stop=True))  # 开多
            if s.direction in (DIRECTION_SHORT, None):
                # 开空仓
                if shortSo and shortSo.price == self.shortIn:
                    # 已经有多单了，不需要再下多单
                    pass
                else:
                    if shortSo:
                        s.log.info('撤开空单重新下')
                        s.cancelOrder(shortSo.stopOrderID)
                    vtOrderIDs.extend(s.short(self.shortIn, s.hands, stop=True))  # 开空
            for _id in vtOrderIDs:
                # 映射
                so = s.ctaEngine.workingStopOrderDict[_id]
                s.stopOrderID2Unit[_id] = self
                self.openStopOrder[so.stopOrderID] = so

    # def orederCloseOnStart(self):
    #     """
    #     启动时下平仓单
    #     :return:
    #     """
    #     s = self.strategy
    #     vtOrderIDs = []
    #     if self.isFull() or self.isTrading():
    #         if self.pos > 0:
    #             # 平多单
    #             # self.longIn - self.atr
    #             price = s.stopPrice or self.longOut
    #             vtOrderIDs.extend(
    #                 s.sell(price, abs(self.pos), stop=True)
    #             )
    #         else:
    #             # 平空单
    #             price = s.stopPrice or self.shortOut
    #             vtOrderIDs.extend(
    #                 s.cover(price, abs(self.pos), stop=True)
    #             )
    #         for _id in vtOrderIDs:
    #             # 映射
    #             s.orderID2Unit[_id] = self
    #             so = s.ctaEngine.workingStopOrderDict[_id]
    #             unit = s.orderID2Unit[so.stopOrderID]
    #             assert isinstance(unit, Unit)
    #             unit.openStopOrder[so.stopOrderID] = so

    def setStatus(self, status):
        """

        :return:
        """
        self.strategy.log.info('{}'.format(self))
        self.strategy.log.info('{} -> {}'.format(self.status, status))
        self.status = status
