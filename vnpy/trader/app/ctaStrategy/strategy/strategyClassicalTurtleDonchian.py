# encoding: UTF-8


from __future__ import division

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
    className = u'ClassicalTurtleDonchianStrategy'
    name = u'经典海龟：唐奇安通道策略'
    author = u'lamter'

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
        'bigShortIn'
        'bigLongOut',
        'bigShortOut'
        'atr',
        'status',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    STATUS_SMALL = u'小周期'
    STATUS_BIG = u'大周期'

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
        self.stopOrderID2Unit = {}
        self.orderID2Unit = {}

    def initMaxBarNum(self):
        barNum = 0
        for p in self.paramList:
            if '_' in p:
                barNum = max(barNum, getattr(self, p))

        self.maxBarNum = barNum * 2

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.maxBarNum)

        self.log.info(u'即将加载 {} 条 bar 数据'.format(len(initData)))

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
            self.log.info(u'初始化完成')
        else:
            self.log.warning(u'初始化数据不足!')

        self.updateHands()

        self.isCloseoutVaild = True
        self.putEvent()

    # ----------------------------------------------------------------------
    @exception
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.log.info(u'%s策略启动' % self.name)

        if not self.isBackTesting():
            # 实盘，可以存库。
            self.saving = True

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
        # todo 基于小周期开仓
        # self.cancelAll()
        # for u in self.units:
        #     u.orderOnStart()

    def orderOnBar(self):
        """
        在策略启动时下单
        :return:
        """
        # todo 基于小周期开仓
        # self.cancelAll()
        # for u in self.units:
        #     u.orderOnBar()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.saveDB()
        self.log.info(u'%s策略停止' % self.name)
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
        self.bm.updateXminBar(bar)
        if self.isCloseoutVaild and self.rtBalance < 0:
            # 爆仓，一键平仓
            self.closeout()

        if self.trading:
            self.cancelAll()
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

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

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
        for indName, [dtList, dataList] in self.techIndLine.items():
            data = getattr(self, indName)
            dtList.append(dt)
            dataList.append(data)

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        log = self.log.info
        if order.status == STATUS_REJECTED:
            log = self.log.warning
            message = u''
            for k, v in order.rawData.items():
                message += u'{}:{}\n'.format(k, v)
            log(message)

            # 补发
            self.orderUntilTradingTime()

        log(u'状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        originCapital, charge, profile = self._onTrade(trade)

        unit = self.orderID2Unit.get(trade.vtOrderID)
        assert isinstance(unit, Unit)

        posChange = self.pos - self.prePos
        assert posChange != 0

        if unit.isEmpty():
            # 开仓
            # unit.status = unit.STATUS_OPEN_TRADING
            # 计算持仓成本
            unit.openTurnover = trade.price * trade.volume
        elif unit.isFull():
            # 已经处于满仓状态，开始进行平仓操作
            # 平仓成本
            unit.closeTurnover = trade.price * trade.volume
        else:  # unit.isTrading()
            if self.pos > 0 and posChange > 0:
                # 加仓
                unit.openTurnover += trade.price * trade.volume
            else:
                # 减仓
                unit.closeTurnover += trade.price * trade.volume

        # 仓位操作
        unit.pos += posChange

        if unit.isEmpty():
            # 平仓完毕
            # 计算盈亏
            if posChange < 0:
                # 平多
                unit.profile = self.roundToPriceTick((unit.closeTurnover - unit.openTurnover) / unit.hands)
            else:
                unit.profile = self.roundToPriceTick((unit.openTurnover - unit.closeTurnover) / unit.hands)

            unit.hands = None
            unit.closeTurnover = unit.openTurnover = None
            unit.longIn = unit.shortIn = None

        # 是否全部完成了平仓：全部为空
        isCloseAll = False not in [u.isEmpty() for u in self.units]

        if isCloseAll:
            # 全部平仓完成
            profile = sum([u.profile for u in self.units if u.profile])
            if profile > 0:
                # 上次盈利了，进入大周期
                self.setStatus(self.STATUS_BIG)
            else:
                # 没盈利，进入小周期
                self.setStatus(self.STATUS_SMALL)

            for u in self.units:
                u.profile = None

        # if arrow.get('2018-11-30 11:00+08').datetime <= self.bar.datetime <= arrow.get(
        #         '2018-11-30 13:59:00+08').datetime:

        self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile)
        print(posChange)
        print(unit)
        time.sleep(0.1)

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

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
        assert isinstance(so, StopOrder)
        if so.status == STOPORDER_CANCELLED:
            # 撤单
            self.stopOrderID2Unit.pop(so.stopOrderID)
        elif so.status == STOPORDER_TRIGGERED:
            # 触发
            unit = self.stopOrderID2Unit.pop(so.stopOrderID)
            self.orderID2Unit[so.vtOrderID] = unit

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

    # STATUS_EMPTY = u'空仓'
    # STATUS_OPEN_TRADING = u'开仓中'
    # STATUS_FULL = u'满仓'
    # STATUS_CLOSE_TRADING = u'平仓中'

    def __init__(self, index, strategy):
        assert isinstance(strategy, ClassicalTurtleDonchianStrategy)
        self.strategy = strategy
        self.index = index
        self.pos = 0  # 多正空负
        # self.openPrice = None  # 技术开仓价

        self.atr = None  # 开仓时的 ATR
        self.vtOrderIDs = []
        self.hands = None
        self.openTurnover = 0  # 持仓成本
        self.closeTurnover = 0  # 平仓成本

        self.profile = None  # 上次盈利
        self.longIn = None  # 技术点位
        self.longOut = None  # 技术点位
        self.shortIn = None  # 技术点位
        self.shortOut = None  # 技术点位

    @property
    def price(self):
        try:
            return self.openTurnover / abs(self.pos)
        except ZeroDivisionError:
            return None
        except TypeError:
            return None

    @property
    def isFirst(self):
        return self.strategy.firstUnit == self

    def orderOnStart(self):
        """
        开盘挂单
        :return:
        """
        try:
            # 检查策略仓位
            if self.pos > 0:
                # 挂平多单
                vtOrderIDs = self.orderCloseLong()
            elif self.pos < 0:
                # 挂平空单
                vtOrderIDs = self.orderCloseShort()
            else:  # self.pos == 0
                # 挂开仓单
                vtOrderIDs = self.orderOpen()
        except TypeError:
            s = self.strategy
            print(141414141)
            print(s.vtSymbol)
            print(s.bar.datetime)
            print(s.longIn, s.am.atr(s.ATR_N))
            print(s._setting)
            raise

        for _id in vtOrderIDs:
            self.strategy.stopOrderID2Unit[_id] = self

    def orderOnBar(self):
        """
        开盘挂单
        :return:
        """
        self.orderOnStart()

    def orderCloseLong(self):
        """
        平多单
        :return:
        """
        s = self.strategy
        longIn = None
        for u in s.units:
            if u.price:
                longIn = u.price
            else:
                break

        # 计算止损价位
        try:
            stopPrice = longIn - self.atr * s.STOP_ATR
        except TypeError:
            print(1515151, self.index)
            for u in s.units:
                print(u.index, u.pos, u.price, u.openTurnover)
            raise

        # 计算止盈价位
        longOut = s.longOut if s.isSmall() else s.bigLongOut

        price = max(stopPrice, longOut)

        vtOrderIDs = s.sell(price, abs(self.pos), stop=True)

        return vtOrderIDs

    def orderCloseShort(self):
        """
        平空单
        :return:
        """
        s = self.strategy

        # 计算止损价位
        shortIn = None
        for u in s.units:
            if u.price:
                shortIn = u.price
            else:
                break
        try:
            stopPrice = shortIn + self.atr * s.STOP_ATR
        except TypeError:
            print(1515151, self.index)
            for u in s.units:
                print(u.index, u.pos, u.price, )
            raise

        # 计算止盈价位
        shortOut = s.shortOut if s.isSmall() else s.bigShortOut

        price = min(stopPrice, shortOut)

        vtOrderIDs = s.cover(price, abs(self.pos), stop=True)
        # if arrow.get('2018-11-29 00:00:00+08').datetime <= s.bar.datetime <= arrow.get('2018-12-01 00:00:00+08').datetime:
        #     s.log.warning(u'U:{} 平空 shortIn: {} atr:{} price:{}'.format(self.index, s.roundToPriceTick(self.shortIn), s.roundToPriceTick(self.atr), s.roundToPriceTick(price)))

        return vtOrderIDs

    def orderOpen(self):
        """
        挂开仓单
        :return:
        """
        s = self.strategy

        vtOrderIDs = []

        if self.isFirst:
            longIn, shortIn = (s.longIn, s.shortIn) if s.isSmall() else (s.bigLongIn, s.bigShortIn)
            self.atr = s.atr

        else:
            longIn, shortIn = s.firstUnit.longIn, s.firstUnit.shortIn
            self.atr = s.firstUnit.atr

        self.hands = s.hands

        # 开多单
        if longIn:
            lprice = longIn + (self.index * s.ADD_ATR) * self.atr
            _vtOrderIDs = s.buy(lprice, self.hands, stop=True)
            self.longIn = lprice
            vtOrderIDs.extend(_vtOrderIDs)

        # 开空单
        if shortIn:
            sprice = shortIn - (self.index * s.ADD_ATR) * self.atr
            _vtOrderIDs = s.short(sprice, self.hands, stop=True)
            self.shortIn = sprice
            vtOrderIDs.extend(_vtOrderIDs)

        if arrow.get('2018-11-30 11:00+08').datetime <= s.bar.datetime <= arrow.get('2018-11-30 13:59:00+08').datetime:
            s.log.warning(u'U:{} 开仓单 shortIn: {} longIn:{} atr:{} lprice:{} sprice:{}'.format(self.index,
                                                                                              *[s.roundToPriceTick(v)
                                                                                                for v in (self.shortIn,
                                                                                                          self.longIn,
                                                                                                          self.atr,
                                                                                                          lprice,
                                                                                                          sprice)]))
        return vtOrderIDs

    def toSave(self):
        """

        :return:
        """
        return {
            'pos': self.pos
        }

    def loadCtaDB(self, dic):
        for k, v in dic.items():
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
        s = super(Unit, self).__str__()[:-1]
        for k in ['index', 'pos', 'openTurnover', 'closeTurnover', 'longIn', 'longOut', 'shortIn', 'shortOut']:
            s += ' {}:{}'.format(k, getattr(self, k))
        return s + '>'
