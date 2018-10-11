# encoding: UTF-8

"""
一个震荡策略，使用通道信号（如唐奇安通道，布林带等）构建一个通道。
突破大周期通道时开仓，并在小盈利时止盈，大浮亏时止损。
意在构建一个小盈多赢的震荡策略。
"""

from __future__ import division

from threading import Timer
from collections import OrderedDict
import time

from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue, exception, logDate
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class ContrarianAtrStrategy(CtaTemplate):
    """反转ATR策略"""
    className = u'反转ATR策略'
    author = u'lamter'

    # 策略参数
    longBar = 20
    n = 1  # 高点 n atr 算作反转
    risk = 0.05  # 每笔风险投入
    flinch = 0  # 畏缩指标
    fixhands = 0  # 固定手数

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'n',
        'longBar',
        'risk',
        'fixhands',
    ])

    # 策略变量
    high = None  # 高点
    low = None  # 低点
    atr = 0  # ATR
    stop = None  # 止损投入
    highBalance = None # 净值高点

    # 变量列表，保存了变量的名称
    _varList = [
        'highBalance',
        'winCount',
        'loseCount',
        'high',
        'low',
        'hands',
        'atr',
        'stop',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(ContrarianAtrStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands
        self.justOpen = False  # 刚开仓过
        self.longStopOrder = None  # 开多停止单实例
        self.shortStopOrder = None  # 开空停止单实例

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
            self.tradingDay = bar.tradingDay
            self.onBar(bar)
            self.bm.preBar = bar

        # self.log.warning(u'加载的最后一个 bar {}'.format(bar.datetime))

        if len(initData) >= self.maxBarNum:
            self.log.info(u'初始化完成')
        else:
            self.log.info(u'初始化数据不足!')

        if self.stop is None:
            # 要在读库完成后，设置止损额度，以便控制投入资金的仓位
            self.updateStop()

        if self.bar:
            self.high = self.high or self.bar.close
            self.low = self.low or self.bar.close

        self.highBalance = self.highBalance or self.rtBalance

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
        if self.high and self.low:
            # 开仓单和平仓单
            self.cancelAll()
            if self.isBackTesting():
                self.orderOpenOnBar()
            self.orderClose()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
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

        # 先撤单再下单
        if self.trading:
            if self.high is None and self.low is None:
                self.high = bar.high
                self.low = bar.low
            elif self.justOpen:
                if self.pos > 0:
                    self.high = max(bar.close, self.high)
                elif self.pos < 0:
                    self.low = min(bar.close, self.low)
                self.justOpen = False
            else:
                self.high = max(bar.high, self.high)
                self.low = min(bar.low, self.low)

            # self.log.info(u'{} {} {} {} {} '.format(self.pos, self.high, bar.high, bar.low, self.low))

            self.cancelAll()
            if self.isBackTesting():
                self.orderOpenOnBar()  # 开仓单
            else:
                if self.pos == 0:
                    self.orderOpenOnBar()  # 开仓单
            self.orderClose()  # 平仓单

    def orderOpenOnBar(self):
        # 开仓价
        longPrice, shortPrice = self.getPrice()

        self.updateHands()

        if self.pos >= 0:
            # 多仓时反手
            shortStopOrderID, = self.short(shortPrice, self.hands, stop=True)
            self.shortStopOrder = self.ctaEngine.workingStopOrderDict[shortStopOrderID]
        if self.pos <= 0:
            longStopOrderID, = self.buy(longPrice, self.hands, stop=True)
            self.longStopOrder = self.ctaEngine.workingStopOrderDict[longStopOrderID]

    def orderOpenOnTrade(self):
        # 开仓价
        self.updateHands()

        if self.prePos > 0:
            # 反手开空
            self.short(self.bm.lastTick.lowerLimit, self.hands)
        elif self.prePos < 0:
            # 反手开多
            self.buy(self.bm.lastTick.upperLimit, self.hands)
        else:
            self.log.warning(u'之前仓位为 pos == 0 无法判断反手方向')


    def getPrice(self):
        # 更新高、低点
        shortPrice = self.roundToPriceTick(self.high - self.atr * self.n)
        longPrice = self.roundToPriceTick(self.low + self.atr * self.n)
        return longPrice, shortPrice

    def orderClose(self):
        """
        平仓单
        :return:
        """
        if self.pos == 0:
            return

        longPrice, shortPrice = self.getPrice()

        if self.pos > 0:
            self.sell(shortPrice, abs(self.pos), stop=True)
        elif self.pos < 0:
            self.cover(longPrice, abs(self.pos), stop=True)

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

        # 通道中线
        self.atr = am.atr(self.longBar)

        # # 通道内最高点
        # self.high, self.low = am.donchian(self.longBar)

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

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

            self.log.warning(u'\n'.join(textList))
        if self.isBackTesting():
            if self.capital <= 0:
                # 回测中爆仓了
                self.capital = 0

        # 重置高低点
        if self.pos > 0:
            # 开多了，开仓点设为高点，高点下跌 n ATR 反手
            self.high = trade.price
        elif self.pos < 0:
            # 开空了，开仓点设为低点，低点反弹 n ATR 反手
            self.low = trade.price

        if self.pos == 0:
            # log = u'{} {} {} v: {}\tp: {}\tb: {}'.format(trade.direction, trade.offset, trade.price, trade.volume,
            #                                              profile, int(self.rtBalance))
            # self.log.warning(log)

            # 平仓了，开始对连胜连败计数
            if profile > 0:
                self.winCount += 1
                self.loseCount = 0
            else:
                self.winCount = 0
                self.loseCount += 1

                # 重新计算风险投入
                self.updateStop()

            self.highBalance = max(self.highBalance, self.rtBalance)
            self.log.info(u'{} {}'.format(self.highBalance, self.rtBalance))

        if self.isBackTesting():
            # 回测时
            self.orderOpenOnTradBackting()
        else:
            # 实盘
            if self.pos == 0:
                self.orderOpenOnTrade()
            else:
                self.cancelAll()
                self.orderClose()

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    def orderOpenOnTradBackting(self):
        if self.pos == 0:
            # 直接更改开仓单
            self.updateHands()
            if self.longStopOrder:
                self.longStopOrder.volume = self.hands
            if self.shortStopOrder:
                self.shortStopOrder.volume = self.hands
        else:
            # 开仓，撤单重发
            self.cancelAll()
            self.orderOpenOnBar()
            self.orderClose()
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
            self.hands = 0
            return

        # 理论仓位
        minHands = max(0, int(self.stop / (self.n * self.atr * self.size)))
        if self.hands == 0:
            self.hands = int(self.maxHands / 2)

        # 仓位计算方法 ===========>
        # 固定仓位
        if self.fixhands:
            # 有固定手数时直接使用固定手数
            self.hands = min(self.maxHands, self.fixhands)
            return

        self.hands = min(self.hands, self.maxHands)
        # 连败中一直轻仓，连胜一直满仓
        # self.hands = 1 if self.loseCount else max(1, hands)

        # 直接动态仓位
        # self.hands = max(1, hands)

        # 按连败次数加仓
        # self.hands = self._calHandsByLoseCountPct(hands, self.flinch)

        # 连败 flinch 次后满仓
        # self.hands = self._calHandsByLoseCount(hands, self.flinch)
        # <==================
    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(ContrarianAtrStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic


    def loadCtaDB(self, document=None):
        super(ContrarianAtrStrategy, self).loadCtaDB(document)
        self._loadVar(document)

    def updateStop(self):
        self.log.info(u'调整风险投入')
        self.stop = self.capital * self.risk
