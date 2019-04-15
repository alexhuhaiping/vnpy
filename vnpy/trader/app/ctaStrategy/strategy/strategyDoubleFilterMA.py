# encoding: UTF-8

"""
做一条MA，然后MA上最近3个MA都是涨的， 并且最近两K线都是收阳， 就开多
"""



from threading import Timer
from collections import OrderedDict
import time

import numpy as np
import arrow

from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue, exception, logDate
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class DoubleFilterMAStrategy(CtaTemplate):
    """日均线策略"""
    className = '二重过滤均线策略'
    author = 'lamter'

    barXmin = 60  # 1小时K线
    longBar = 55  # 5均线
    trendMA = 5 # 取几个MA值来判断趋势
    fixhands = 1 # 固定手数

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'fixhands',
        'barXmin',
        'longBar',
        'trendMA',
    ])

    # 策略变量

    # 变量列表，保存了变量的名称
    _varList = [
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DoubleFilterMAStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

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

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
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

        ma = self.am.ma(self.longBar, True)

        trend = ma[-self.trendMA:] > ma[-self.trendMA-1:-1]
        assert isinstance(trend, np.ndarray)

        if trend.all():
            # 连续 self.trendMA 个 MA 值都是递增，多趋势
            if self.am.close[-1] > self.am.open[-1] and self.am.close[-2] > self.am.open[-2]:
                if self.pos <= 0:
                    price = self.bar.close
                    self.cancelAll()
                    if self.pos < 0:
                        # 已经有持仓了
                        # 先平仓
                        self.sendOrder(CTAORDER_COVER, price, abs(self.pos), stop=True)
                    # 开仓
                    self.sendOrder(CTAORDER_BUY, price, self.fixhands, stop=True)

        trend = ma[-self.trendMA:] < ma[-self.trendMA - 1:-1]
        assert isinstance(trend, np.ndarray)
        if trend.all():
            # 连续 self.trendMA 个 MA 值都是递减，空趋势
            if self.am.close[-1] < self.am.open[-1] and self.am.close[-2] < self.am.open[-2]:
                if self.pos >= 0:
                    price = self.bar.close
                    self.cancelAll()
                    if self.pos > 0:
                        # 已经有持仓了
                        # 先平仓
                        self.sendOrder(CTAORDER_SELL, price, abs(self.pos), stop=True)
                    # 开仓
                    self.sendOrder(CTAORDER_SHORT, price, self.fixhands, stop=True)

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

        # if trade.offset in OFFSET_CLOSE_LIST:
        #     textList = [u'{} {} {} {}'.format(self.tradingDay, trade.price, trade.direction, trade.offset)]
        #     textList.append(u'资金变化 {} -> {}'.format(originCapital, self.capital))
        #     textList.append(u'仓位{} -> {}'.format(self.prePos, self.pos))
        #     textList.append(u'手续费 {} 利润 {}'.format(round(charge, 2), round(profile, 2)))
        #     textList.append(u'**********************')
        #     print(u'\n'.join(textList))

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        # print(u'{} {}'.format(self.bar.datetime, so))
        self.putEvent()