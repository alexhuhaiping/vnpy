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

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class AtrBottomFishStrategy(CtaTemplate):
    """ATR反转后抄底，不设技术止损"""
    className = '反转ATR抄底策略'
    author = 'lamter'

    # 策略参数
    longBar = 20  #
    n = 1  # 高点 n atr 算作反转
    fixhands = None  # 固定手数

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'longBar',
        'fixhands',
    ])

    # 策略变量
    high = None  # 高点
    low = None  # 低点
    atr = 0  # ATR

    # 变量列表，保存了变量的名称
    _varList = [
        'winCount',
        'loseCount',
        'high',
        'low',
        'hands',
        'atr',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(AtrBottomFishStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands or 0
        self.justOpen = True

        self.longPrice = None
        self.shortPrice = None

    def initMaxBarNum(self):
        self.maxBarNum = self.longBar * 2

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
        if len(initData) >= self.maxBarNum:
            self.log.info('初始化完成')
        else:
            self.log.info('初始化数据不足!')

        if self.bar:
            self.high = self.high or self.bar.close
            self.low = self.low or self.bar.close

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

        # 开盘再下单
        self.orderUntilTradingTime()

        self.putEvent()

    def _orderOnThreading(self):
        if self.high and self.low:
            # 开仓单和平仓单
            self.cancelAll()
            self.orderOnStart()

    def orderOnStart(self):
        # 开仓价
        self.longPrice, self.shortPrice = self.getPrice()

        self.updateHands()

        if self.hands == 0:
            return

        if self.pos == 0:
            # 同时挂多单和空单
            self.buy(self.longPrice, self.hands)
            self.short(self.shortPrice, self.hands)
        elif self.pos > 0:
            # 已有多仓，平多开空
            self.sell(self.shortPrice, abs(self.pos))
            self.short(self.shortPrice, self.hands)
        elif self.pos < 0:
            # 已有空仓，平空开多
            self.buy(self.longPrice, abs(self.pos))
            self.cover(self.longPrice, self.hands)

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
        self.bm.updateXminBar(bar)
        if self.isCloseoutVaild and self.rtBalance < 0:
            # 爆仓，一键平仓
            self.closeout()

        if self.trading:
            # 计算高低点
            if self.high is None and self.low is None:
                self.high = bar.high
                self.low = bar.low
            elif self.justOpen:
                if self.pos > 0:
                    self.low = min(bar.close, self.low)
                elif self.pos < 0:
                    self.high = max(bar.close, self.high)

                self.justOpen = False
            else:
                self.high = max(bar.high, self.high)
                self.low = min(bar.low, self.low)

                # self.log.info(u'{} {} {} {} {} '.format(self.pos, self.high, bar.high, bar.low, self.low))

            # 两者只能选择一个
            self.orderWithoutCheckCanOrder()
            # self.checkCanOrcer(bar)


    def orderWithoutCheckCanOrder(self):
        """

        :return:
        """
        # orderOnBar 中会做撤单操作
        self.orderOnBar()  # 开仓单
        self.saveDB()

    def checkCanOrcer(self, bar):
        if bar.datetime.hour == 14 and bar.datetime.minute >= 58:
            # 清仓
            self.cancelAll()
            self.canOrder = False
            self.clearAll()
        elif bar.high - bar.low < self.atr:
            # 下单/重新下单
            # 先撤单再下单
            self.orderOnBar()  # 开仓单

            self.saveDB()
        else:
            pass

    def orderOnBar(self):
        # 开仓价

        longPrice, shortPrice = self.getPrice()

        self.updateHands()

        if self.hands == 0:
            return

        # 价格变化了，需要撤单重新下单
        if self.pos == 0:
            # 同时挂多单和空单
            if self.longPrice != longPrice or self.shortPrice != shortPrice:
                # 价格变化了，撤单按重新下
                self.cancelAll()
                self.buy(longPrice, self.hands)
                self.short(shortPrice, self.hands)
        elif self.pos > 0:
            # 已有多仓，平多开空
            if self.shortPrice != shortPrice:
                self.cancelAll()
                self.sell(shortPrice, abs(self.pos))
                self.short(shortPrice, self.hands)
        else:  # self.pos < 0
            # 已有空仓，平空开多
            if self.longPrice != longPrice:
                self.cancelAll()
                self.buy(longPrice, abs(self.pos))
                self.cover(longPrice, self.hands)

        self.longPrice, self.shortPrice = longPrice, shortPrice

    def orderOnTrade(self):
        if self.hands == 0:
            return

        # 开仓价
        longPrice, shortPrice = self.getPrice()
        self.updateHands()

        if self.hands == 0:
            return

        # 价格变化了，需要撤单重新下单
        if self.pos == 0:
            # 同时挂多单和空单
            if self.longPrice != longPrice or self.shortPrice != shortPrice:
                # 价格变化了，撤单按重新下
                self.cancelAll()
                self.buy(longPrice, self.hands)
                self.short(shortPrice, self.hands)
        elif self.pos > 0:
            # 已有多仓，平多开空
            if self.shortPrice != shortPrice:
                self.cancelAll()
                self.sell(shortPrice, abs(self.pos))
                self.short(shortPrice, self.hands)
        else:  # self.pos < 0
            # 已有空仓，平空开多
            if self.longPrice != longPrice:
                self.cancelAll()
                self.buy(longPrice, abs(self.pos))
                self.cover(longPrice, self.hands)

        self.longPrice, self.shortPrice = longPrice, shortPrice

    def getPrice(self):
        # 更新高、低点
        longPrice = self.roundToPriceTick(self.high - self.atr * self.n)
        shortPrice = self.roundToPriceTick(self.low + self.atr * self.n)
        # return longPrice + self.priceTick * 5, shortPrice - self.priceTick * 5
        return longPrice , shortPrice

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

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

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

        # 重置高低点
        # if self.pos > 0:
        #     # 开多了，开仓点设为底点，低点上涨 n ATR 止盈并反手
        #     self.low = trade.price
        # elif self.pos < 0:
        #     # 开多了，开仓点设为高点，高点下跌 n ATR 止盈并反手
        #     self.high = trade.price

        if self.pos == 0:
            self.high = self.low = trade.price

            # 平仓了，开始对连胜连败计数
            if profile > 0:
                self.winCount += 1
                self.loseCount = 0
            else:
                self.winCount = 0
                self.loseCount += 1

        if self.pos == 0:
            # 平仓成交，不处理
            pass
        else:
            # 开仓成交
            self.orderOnTrade()

        # self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile)

        # 发出状态更新事件
        self.saveDB()
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
        dic = super(AtrBottomFishStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic

    def loadCtaDB(self, document=None):
        super(AtrBottomFishStrategy, self).loadCtaDB(document)
        self._loadVar(document)
