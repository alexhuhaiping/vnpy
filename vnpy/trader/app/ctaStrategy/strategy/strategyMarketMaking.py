# encoding: UTF-8

"""
做市商策略
"""

from __future__ import division

import datetime
from threading import Timer
from collections import OrderedDict, defaultdict
import time

import arrow

import tradingtime as tt
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import exception
from vnpy.trader.vtObject import VtTradeData, VtTickData, VtOrderData
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class MarketMakingStrategy(CtaTemplate):
    """做市商策略"""
    className = u'做市商策略'
    author = u'lamter'

    # 策略参数
    shiftingOpen = 1  # 挂单位置从买一/卖一偏移的价格
    shiftingClose = 2  # 挂单位置从买一/卖一偏移的价格
    reorder = 4  # 已经挂单的偏离买一/卖一的价格后要撤单重新下单

    fixhands = 1
    stopHands = 5  # hands 单边达到几手时停止开仓
    stopDelta = 5  # sec 允许敞口持续多久

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'fixhands',
        'stopHands',
        'stopDelta',
    ])

    orderCount = 0  # 统计下单次数
    cancelCount = 0  # 撤单次数

    # 变量列表，保存了变量的名称
    ask1 = bid1 = None
    askVol1 = bidVol1 = None
    _varList = [
        'bidVol1','bid1',
        'ask1','askVol1',
        'status',
        'orderCount', 'cancelCount',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    STATUS_READY = u'预备'  # 没有持仓，没有下单
    STATUS_WAIT_DEAL = u'等待成交'  # 已经下单，等待成交
    STATUS_REPLENISH = u'补仓'  # 出现单腿成交，需要补仓
    STATUS_RISK_WARNING = u'敞口预警'  # 风险敞口达到最大，进入清仓预警
    STATUS_PAUSE = u'暂停'  # 暂停onTick下单，但是策略没有停止

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(MarketMakingStrategy, self).__init__(ctaEngine, setting)
        is_tradingtime, self.tradingDay = tt.get_tradingday(arrow.now().datetime)

        class StatusPause:
            strategy = self

            def __init__(self, status):
                self.status = status

            def __enter__(self):
                self.strategy.setStatus(self.strategy.STATUS_PAUSE)
                return self  # 可以返回不同的对象

            def __exit__(self, exc_type, exc_value, exc_tb):
                self.strategy.setStatus(self.status)
                return

        self.pause = StatusPause

        self.orders = {}  # {'vtOrderID': vtOrder()}
        self.status = self.STATUS_PAUSE

    @property
    def tick(self):
        return self.bm.lastTick

    def initMaxBarNum(self):
        # todo
        self.maxBarNum = 10

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.loadBarOnInit()

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

        # 交易时间段再下单
        self.orderUntilTradingTime()

        self.putEvent()

    def _orderOnThreading(self):
        self.setStatus(self.STATUS_READY)

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.log.info(u'%s策略停止' % self.name)
        self.setStatus(self.STATUS_PAUSE)
        self.cancelAll()
        self.putEvent()

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

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        assert isinstance(tick, VtTickData)
        self.bm.updateTick(tick)
        self.bid1 = tick.bidPrice1
        self.ask1 = tick.askPrice1
        self.bidVol1 = tick.bidVolume1
        self.askVol1 = tick.askVolume1

        if self.trading and self.status != self.STATUS_PAUSE:
            # self.bm.updateTick(tick)
            if self.status == self.STATUS_READY:
                # 尚未下单，开始下单
                self.orderOpenStatuStartOnTick(tick)
            elif self.status == self.STATUS_REPLENISH:
                # 需要补仓
                self.orderOpenStatusReplenishOnTick(tick)
            elif self.status == self.STATUS_RISK_WARNING:
                if datetime.datetime.now() > self.stopMoment:
                    # 风险持续过大，强平
                    self.orderCloseAllOnTick(tick)
            #
            with self.pause(self.status):
                # 检查头寸和平仓单，将平仓单补齐至头寸
                self.orderCloseOnTick(tick)

                # 对偏移过远的订单撤单
                self.cancelOnTick(tick)

    def orderCloseAllOnTick(self, tick):
        """
        全部平仓
        :return:
        """
        with self.pause(self.STATUS_RISK_WARNING):
            if self.orders:
                self.cancelAll()
                return

            if self.positionDetail.longPos:
                orderType = CTAORDER_SELL
                price = tick.bidPrice1 - self.priceTick * 2
                self.sendOrder(orderType, price, self.positionDetail.longPos)

            if self.positionDetail.shortPos:
                orderType = CTAORDER_COVER
                price = tick.askPrice1 + self.priceTick * 2
                self.sendOrder(orderType, price, self.positionDetail.shortPos)


    def orderCloseOnTick(self, tick):
        shortVolume = longVolume = 0
        for o in self.orders.values():
            if o.status == STATUS_NOTTRADED and o.offset in OFFSET_CLOSE_LIST:
                if o.direction == DIRECTION_SHORT:
                    longVolume += 1
                if o.direction == DIRECTION_LONG:
                    shortVolume += 1

        if self.positionDetail.shortPos:
            volume = self.positionDetail.shortPos - shortVolume
            if volume < 0:
                self.log.warning(u'平仓 空 单超过持仓数量 {} {}'.format(self.positionDetail.shortPos, shortVolume))
            else:
                for i in range(volume):
                    # 买平
                    self._orderClose(CTAORDER_COVER, tick)
        if self.positionDetail.longPos:
            volume = self.positionDetail.longPos - longVolume
            if volume < 0:
                self.log.warning(u'平仓 多 单超过持仓数量 {} {}'.format(self.positionDetail.longPos, longVolume))
            else:
                for i in range(volume):
                    # 买平
                    self._orderClose(CTAORDER_SELL, tick)

    def _orderClose(self, orderType, tick):
        if orderType == CTAORDER_COVER:
            # 买平
            price = tick.bidPrice1 - self.shiftingClose * self.priceTick
        elif orderType == CTAORDER_SELL:
            # 卖平
            price = tick.askPrice1 + self.shiftingClose * self.priceTick
        else:
            self.log.warning(u'未知的平仓模式 orderType == {}'.format(orderType))
            return
        self.sendOrder(orderType, price, self.fixhands)

    def cancelOnTick(self, tick):
        for o in self.orders.values():
            if o.status == STATUS_NOTTRADED:
                if o.direction == DIRECTION_LONG:
                    # 多单，对比买一价
                    if o.price < tick.bidPrice1 - self.priceTick * self.reorder:
                        self.cancelOrder(o.vtOrderID)
                elif o.direction == DIRECTION_SHORT:
                    if o.price > tick.askPrice1 + self.priceTick * self.reorder:
                        self.cancelOrder(o.vtOrderID)

    def orderOpenStatusReplenishOnTick(self, tick):
        """

        :param tick:
        :return:
        """
        with self.pause(self.STATUS_WAIT_DEAL):
            # 统计所有订单，看看缺少什么方向的开仓单
            directions = defaultdict(lambda: 0)
            for o in self.orders.values():
                # 检查需要补仓的方向
                if o.offset == OFFSET_OPEN and o.status == STATUS_NOTTRADED:
                    if o.direction == DIRECTION_LONG:
                        directions[DIRECTION_LONG] += 1
                    elif o.direction == DIRECTION_SHORT:
                        directions[DIRECTION_SHORT] += 1
                    else:
                        self.log.warning(u'补仓失败，未知的开仓方向')
            if directions[DIRECTION_LONG] == 0:
                self.orderOpen(DIRECTION_LONG, tick.bidPrice1)
            if directions[DIRECTION_SHORT] == 0:
                # 缺少空单# 卖开
                self.orderOpen(DIRECTION_SHORT, tick.askPrice1)

    def orderOpenStatuStartOnTick(self, tick):

        with self.pause(self.STATUS_WAIT_DEAL):
            # 买开
            self.orderOpen(DIRECTION_LONG, tick.bidPrice1)
            # 卖开
            self.orderOpen(DIRECTION_SHORT, tick.askPrice1)

    def orderOpen(self, direction, price):
        if direction == DIRECTION_LONG:
            if self.pos < 0:
                self.log.info(u'已经持有空头，不再挂买开，只挂买平')
                return
            else:
                price = price - self.priceTick * self.shiftingOpen
                # TODO 涨停价不再卖出
                orderType = CTAORDER_BUY


        elif direction == DIRECTION_SHORT:
            if self.pos > 0:
                self.log.info(u'已经持有多头，不再挂卖开，只挂卖平')
                return
            else:
                price = price + self.priceTick * self.shiftingOpen
                orderType = CTAORDER_SHORT
                # TODO 涨停价不再卖出
        else:
            self.log.warning(u'挂开仓单失败，未知的条件 direction:{} pos:{}'.format(direction, self.pos))
            return

        self.log.info(u'READY 挂开仓单 {} {} {}'.format(orderType, price, self.fixhands))

        return self.sendOrder(orderType, price, self.fixhands)

    def setStatus(self, status):
        self.status = status
        if status == self.STATUS_RISK_WARNING:
            self.log.info(u'风险敞口预警 pos:{}'.format(self.pos))

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        assert isinstance(order, VtOrderData)
        log = self.log.info
        if order.status == STATUS_REJECTED:
            # 拒单
            log = self.log.warning
            message = u''
            for k, v in order.rawData.items():
                message += u'{}:{}\n'.format(k, v)
            log(message)

        elif order.status == STATUS_NOTTRADED:
            # 下单成功
            if order.vtOrderID and order.vtOrderID not in self.orders:
                self.orders[order.vtOrderID] = order
        elif order.status == STATUS_CANCELLED:
            # 已经撤销
            self.cancelCount += 1
            try:
                self.orders.pop(order.vtOrderID)
            except KeyError:
                pass
        elif order.status == STATUS_UNKNOWN:
            log(u'状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))
        elif order.status == STATUS_ALLTRADED:
            # 全部成交
            try:
                self.orders.pop(order.vtOrderID)
            except KeyError:
                pass

    def orderCloseOnTrade(self, trade):
        """

        :param order:
        :return:
        """
        assert isinstance(trade, VtTradeData)

        if trade.direction == DIRECTION_LONG:
            # 成交的买多, 下卖平单
            price = trade.price + self.shiftingClose * self.priceTick
            self.sendOrder(CTAORDER_SELL, price, self.fixhands)
        elif trade.direction == DIRECTION_LONG:
            # 成交的是买空, 下买平单
            price = trade.price - self.shiftingClose * self.priceTick
            self.sendOrder(CTAORDER_COVER, price, self.fixhands)
        else:
            self.log.warning(u'未知的开仓方向')
            return

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        self._onTrade(trade)

        if trade.offset == OFFSET_OPEN and self.status != self.STATUS_RISK_WARNING:
            # 挂平仓单
            self.log.info(u'{} 成交 价格:{}'.format(trade.offset, trade.price))
            self.orderCloseOnTrade(trade)
            if abs(self.pos) < self.stopHands:
                # 风险敞口未达到止损
                # 设置状态为需要补充
                self.setStatus(self.STATUS_REPLENISH)
            else:
                # 将状态设置为敞口预警
                self.setStatus(self.STATUS_RISK_WARNING)
                self.stopMoment = datetime.datetime.now() + datetime.timedelta(seconds=self.stopDelta)

        if trade.offset in OFFSET_CLOSE_LIST and self.status != self.STATUS_RISK_WARNING:
            # 平仓了，重新开仓
            self.setStatus(self.STATUS_REPLENISH)

        if self.pos == 0 and self.status == self.STATUS_RISK_WARNING:
            self.log.info(u'重新开始下单')
            self.setStatus(self.STATUS_PAUSE)

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""

        self.putEvent()

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(MarketMakingStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic

    def loadCtaDB(self, document=None):
        super(MarketMakingStrategy, self).loadCtaDB(document)
        if document and self.tradingDay == document['tradingDay']:
            # 重置下单和撤单次数
            self.orderCount = document['orderCount']
            self.cancelCount = document['orderCount']

    def sendOrder(self, orderType, price, volume, stop=False, stopProfile=False):
        self.orderCount += 1
        return super(MarketMakingStrategy, self).sendOrder(orderType, price, volume, stop, stopProfile)
