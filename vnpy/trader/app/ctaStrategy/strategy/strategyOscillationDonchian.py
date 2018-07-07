# encoding: UTF-8

"""
一个震荡策略，使用通道信号（如唐奇安通道，布林带等）构建一个通道。
突破大周期通道时开仓，并在小盈利时止盈，大浮亏时止损。
意在构建一个小盈多赢的震荡策略。
"""

from __future__ import division

from threading import Timer
import traceback
from collections import OrderedDict
import time
import datetime

import tradingtime as tt
import arrow

from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue, exception, logDate
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class OscillationDonchianStrategy(CtaTemplate):
    """震荡策略"""
    className = 'OscillationDonchianStrategy'
    author = u'lamter'

    # 策略参数
    longBar = 20
    stopProfile = 1
    stopLoss = 4
    slippageRate = 1 / 0.2  # 盈利空间和滑点的比例
    # initDays = 10  # 初始化数据所用的天数
    fixedSize = 1  # 每次交易的数量
    risk = 0.05  # 每笔风险投入
    flinch = 3  # 连胜 flinch 次后畏缩1次

    # 策略变量
    longHigh = 0  # 大周期高点
    longLow = 0  # 大周期低点
    middle = 0  # 大周期中线
    atr = 0  # ATR
    stopProfilePrice = None  # 止盈价格
    stopLossPrice = None  # 止损价格
    stop = None  # 止损投入
    openTag = True  # 是否可以开仓
    openReset = None  # 开仓重置信号

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'flinch',
        'slippageRate',
        'longBar',
        'stopProfile',
        'stopLoss',
        'fixedSize',
        'risk',
    ])

    # 变量列表，保存了变量的名称
    _varList = [
        'winCount',
        'loseCount',
        'hands',
        'openTag',
        'openReset',
        'longHigh',
        'middle',
        'longLow',
        'atr',
        'stopProfilePrice',
        'stopLossPrice',
        'stop',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(OscillationDonchianStrategy, self).__init__(ctaEngine, setting)

        self.reOrder = False # 是否重新下单
        self.ordering = False  # 正处于下单中的标记为
        self.hands = self.fixedSize
        self.balanceList = OrderedDict()

    def initMaxBarNum(self):
        self.maxBarNum = self.longBar * 2

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
            # TOOD 测试代码
            self.tradingDay = bar.tradingDay
            self.onBar(bar)
            self.bm.preBar = bar

        # self.log.info(u'加载的最后一个 bar {}'.format(bar.datetime))

        if len(initData) >= self.maxBarNum:
            self.log.info(u'初始化完成')
        else:
            self.log.info(u'初始化数据不足!')

        if self.stop is None:
            # 要在读库完成后，设置止损额度，以便控制投入资金的仓位
            self.updateStop()

        self.clearOrdering()
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

            # 开盘下单逻辑
            self.orderUntilTradingTime()

        self.putEvent()

    def _orderOnThreading(self):
        """
        在 orderOnTradingTime 中调用该函数，在子线程中下单
        :return:
        """
        self.orderOnXminBar(self.xminBar)

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.log.info(u'%s策略停止' % self.name)
        self.putEvent()
        # self.saveDB()

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

        if not self.openTag:
            # 尝试重置开仓标记位
            if self.prePos > 0:
                self.openTag = bar.close <= self.openReset
            else:
                self.openTag = bar.close >= self.openReset

        if self.reOrder:
            self.reOrder = False
            # 撤掉开仓单
            self.cancelAll()
            if not self.isBackTesting():
                time.sleep(0.5)
            # 下平仓单
            self.orderOnXminBar(self.xminBar)
            # self.log.warning(u'onBar 重新下单 l:{} w:{} h:{}'.format(self.loseCount, self.winCount, self.hands))

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

        # 计算指标数值
        # 通道内第二高点
        # highs = [i for i in am.high[-self.longBar:]]
        # highs.sort()
        # self.longHigh = highs[-2]
        # lows = [i for i in am.low[-self.longBar:]]
        # lows.sort()
        # self.longLow = lows[1]

        # 通道内最高点
        self.longHigh, self.longLow = am.donchian(self.longBar)

        # 通道中线
        self.middle = (self.longHigh + self.longLow) / 2
        self.atr = am.atr(self.longBar)

        if not self.openTag:
            if self.prePos > 0:
                if bar.close < bar.open and bar.high - bar.low > self.atr * 0.5:
                    self.openTag = True
                _high = max(self.longHigh, self.xminBar.high)
                self.openReset = max(self.openReset, _high - self.atr * 0.5)
            else:
                if bar.close > bar.open and bar.high - bar.low > self.atr * 0.5:
                    self.openTag = True
                _low = min(self.longLow, self.xminBar.low)
                self.openReset = min(self.openReset, _low + self.atr * 0.5)

        if self.trading:
            # 下单前先撤单
            self.cancelAll()
            if not self.isBackTesting():
                time.sleep(0.5)

            if not self.isBackTesting():
                # 实盘中，要对是否处于连续交易时间段进行检测
                if self.isOrderInContinueCaution():
                    # 处于连续竞价可直接下单
                    self.orderOnXminBar(bar)
                else:
                    # j
                    self.orderUntilTradingTime()
            else:
                # 回测中直接下单
                self.orderOnXminBar(bar)

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()
        self.log.info(u'更新 XminBar {}'.format(xminBar.datetime))

    @exception
    def orderOnXminBar(self, bar):
        """
        在 onXminBar 中的的指标计算和下单逻辑
        :param am:
        :param bar:
        :return:
        """
        if not self.trading:
            self.log.warn(u'不能下单 trading: False')
            return

        if not self.waitOrdingTag():
            # 下单状态无法释放
            return

        # 计算开仓仓位
        self.updateHands()

        if self.hands == 0:
            self.log.info(u'开仓hands==0，不下单')
            return

        if self.reOrder:
            self.reOrder = False

        # 发送开仓委托
        if self.openTag:
            # 封板时 atr 可能为0，此时不入场
            # 滑点占盈利空间的比例要小于slippageRate
            slippage = self.priceTick * 2
            profile = self.stopProfile * self.atr
            if profile / slippage >= self.slippageRate:
                if self.pos == 0 or self.pos < 0:
                    # 空仓或者持有空头，下多单
                    self.buy(self.longHigh, self.hands, True)
                if self.pos == 0 or self.pos > 0:
                    # 空仓或者持有多头，下空单
                    self.short(self.longLow, self.hands, True)
            else:
                self.log.info(
                    u'{} {} {} atr:{} 过低不开仓'.format(profile, slippage, self.slippageRate, round(self.atr, 2)))

        # 持有多头仓位
        if self.pos > 0:
            if self.stopProfilePrice is None:
                # 止盈价格
                self.stopProfilePrice = self.averagePrice + self.stopProfile * self.atr
            if self.stopLossPrice is None:
                # 止损价格
                self.stopLossPrice = self.averagePrice - self.stopLoss * self.atr
            self.stopLossPrice = max(self.stopLossPrice, self.longLow)

            # 止盈单
            self.sell(self.stopProfilePrice, abs(self.pos), False)
            # 止损单
            self.sell(self.stopLossPrice, abs(self.pos), True)

        # 持有空头仓位
        if self.pos < 0:
            if self.stopProfilePrice is None:
                self.stopProfilePrice = self.averagePrice - self.stopProfile * self.atr

            if self.stopLossPrice is None:
                self.stopLossPrice = self.averagePrice + self.stopLoss * self.atr
            self.stopLossPrice = min(self.stopLossPrice, self.longHigh)

            # 止盈单
            self.cover(self.stopProfilePrice, abs(self.pos), False)
            # 止损单
            self.cover(self.stopLossPrice, abs(self.pos), True)

        if not self.isBackTesting():
            # 下单完毕后3秒才释放订单状态
            def foo():
                self.clearOrdering()

            Timer(0.5, foo).start()

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

        originCapital = preCapital = self.capital

        self.charge(trade.offset, trade.price, trade.volume)

        # 手续费
        charge = preCapital - self.capital

        preCapital = self.capital

        # 回测时滑点
        if self.isBackTesting():
            self.chargeSplipage(trade.volume)

        # 计算成本价和利润
        self.capitalBalance(trade)
        profile = self.capital - preCapital

        if not self.isBackTesting():
            textList = [u'{}{}'.format(trade.direction, trade.offset)]
            textList.append(u'资金变化 {} -> {}'.format(originCapital, self.capital))
            textList.append(u'仓位{} -> {}'.format(self.prePos, self.pos))
            textList.append(u'手续费 {} 利润 {}'.format(round(charge, 2), round(profile, 2)))
            textList.append(
                u','.join([u'{} {}'.format(k, v) for k, v in self.positionDetail.toHtml().items()])
            )

            self.log.info(u'\n'.join(textList))
        if self.isBackTesting():
            if self.capital <= 0:
                # 回测中爆仓了
                self.capital = 0

        if self.pos == 0:
            # log = u'atr:{} {} {} {} {} {} {}'.format(int(self.atr), trade.direction, trade.offset, trade.price,
            #                                          trade.volume,
            #                                          profile, int(self.rtBalance))
            # self.log.warning(log)

            # 重置止盈止损价格
            self.stopLossPrice = None
            self.stopProfilePrice = None

            if profile < 0:
                self.updateStop()
                self.loseCount += 1
                self.winCount = 0

            if profile > 0:
                self.winCount += 1
                self.loseCount = 0
                # 盈利，设置信号
                self.openTag = False
                # 计算重置开仓信号
                if self.prePos > 0:
                    self.openReset = trade.price - self.atr
                else:
                    self.openReset = trade.price + self.atr

        count = 0
        vtOrder = self.getOrder(trade.vtOrderID)
        while vtOrder is None:
            count += 1
            if count >= 10:
                self.log.warning(u'找不到订单对象{}'.format(trade.vtOrderID))
                return

        if vtOrder.totalVolume == trade.volume:
            # 完全成交
            self.reOrder = True

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""

        self.putEvent()

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
            return

        minHands = max(0, int(self.stop / (self.atr * self.stopLoss * self.size)))

        # 2连败后重仓，盈利后立即轻仓
        if self.loseCount < self.flinch:
            minHands = min(int(minHands * self.loseCount /self.flinch), minHands)
            minHands = max(1, minHands)

            # minHands = min(1, minHands)

        # 连胜逐渐建仓，连败后逐渐加仓
        # if self.winCount:
        #     # 连胜后削减仓位
        #     decrRate = 1 - self.winCount * 1/self.flinch
        #     minHands = max(1, minHands * decrRate)
        # if self.loseCount:
        #     # 连败后增加仓位
        #     incrRate = self.loseCount * 0.3
        #     minHands = max(minHands * incrRate, minHands)

        self.hands = min(minHands, self.maxHands)

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(OscillationDonchianStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        dic['openTag'] = int(dic['openTag'])
        return dic

    def loadCtaDB(self, document=None):
        super(OscillationDonchianStrategy, self).loadCtaDB(document)
        if document:
            for k in self._varList:
                try:
                    setattr(self, k, document[k])
                except KeyError:
                    self.log.warning(u'未保存的key {}'.format(k))

        self.openTag = bool(self.openTag)

    def updateStop(self):
        self.log.info(u'调整风险投入')
        self.stop = self.capital * self.risk

    def waitOrdingTag(self):
        if self.isBackTesting():
            # 回测中，无需等待
           return True
        orderCount = 0
        waitSeconds = 0.5 * 20
        while self.ordering:
            self.log.info(u'正处于下单中')
            orderCount += 1
            time.sleep(0.5)
            if orderCount > waitSeconds:
                self.log.warning(u'{}秒内下单状态无法解除'.format(waitSeconds))
                return False

        self.setOrdering()
        return True

    def setOrdering(self):
        if not self.isBackTesting():
            self.ordering = True

    def clearOrdering(self):
        if not self.isBackTesting():
            self.ordering = False
