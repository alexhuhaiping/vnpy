# encoding: UTF-8

"""
感谢Darwin Quant贡献的策略思路。
知乎专栏原文：https://zhuanlan.zhihu.com/p/24448511

策略逻辑：
1. 布林通道（信号）
2. CCI指标（过滤）
3. ATR指标（止损）

适合品种：螺纹钢
适合周期：15分钟

这里的策略是作者根据原文结合vn.py实现，对策略实现上做了一些修改，仅供参考。

"""

from __future__ import division

import arrow

from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class SvtBollChannelStrategy(CtaTemplate):
    """基于布林通道的交易策略"""
    className = 'SvtBollChannelStrategy'
    author = u'用Python的交易员'

    # 策略参数
    bollWindow = 18  # 布林通道窗口数
    bollDev = 3.4  # 布林通道的偏差
    cciWindow = 10  # CCI窗口数
    atrWindow = 30  # ATR窗口数
    slMultiplier = 5.2  # 计算止损距离的乘数
    initDays = 10  # 初始化数据所用的天数
    fixedSize = 1  # 每次交易的数量
    risk = slMultiplier / 100.  # 每笔风险投入

    # 策略变量
    bollUp = 0  # 布林通道上轨
    bollDown = 0  # 布林通道下轨
    cciValue = 0  # CCI指标数值
    atrValue = 0  # ATR指标数值

    intraTradeHigh = 0  # 持仓期内的最高点
    intraTradeLow = 0  # 持仓期内的最低点
    longStop = 0  # 多头止损
    shortStop = 0  # 空头止损

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'bollWindow',
        'bollDev',
        'cciWindow',
        'atrWindow',
        'slMultiplier',
        'initDays',
        'fixedSize',
        'risk',
    ])

    # 变量列表，保存了变量的名称
    varList = CtaTemplate.varList[:]
    _varList = [
        'bollUp',
        'bollDown',
        'cciValue',
        'atrValue',
        'intraTradeHigh',
        'intraTradeLow',
        'longStop',
        'shortStop',
        'hands'
    ]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(SvtBollChannelStrategy, self).__init__(ctaEngine, setting)

        self.hands = self.fixedSize

    def initMaxBarNum(self):
        self.maxBarNum = max(self.atrWindow, self.bollWindow, self.cciWindow)

    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.maxBarNum)

        self.log.info(u'即将加载 {} 条 bar 数据'.format(len(initData)))

        self.initContract()

        for bar in initData:
            self.bm.bar = bar
            self.onBar(bar)

        # self.log.info(u'加载的最后一个 bar {}'.format(bar.datetime))

        if len(initData) >= self.maxBarNum:
            self.log.info(u'初始化完成')
        else:
            self.log.info(u'初始化数据不足!')

        # 从数据库加载策略数据
        if not self.isBackTesting():
            # 需要等待保证金加载完毕
            document = self.fromDB()
            self.loadCtaDB(document)

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.log.info(u'%s策略启动' % self.name)

        if self.xminBar and self.am and self.inited and self.trading:
            self.cancelAll()
            self.orderOnXminBar(self.xminBar)

        if not self.isBackTesting():
            # 实盘，可以存库。
            self.saving = True

        self.putEvent()

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
        """收到Bar推送（必须由用户继承实现）"""
        self.bm.updateBar(bar)
        if self.trading:
            self.log.info(u'更新 bar'.format(bar.datetime))

    # ----------------------------------------------------------------------
    def onXminBar(self, xminBar):
        """收到X分钟K线"""
        bar = xminBar

        # 全撤之前发出的委托
        self.cancelAll()

        # 保存K线数据
        am = self.am

        am.updateBar(bar)

        if not am.inited:
            return

        # 计算指标数值
        self.bollUp, self.bollDown = am.boll(self.bollWindow, self.bollDev)
        self.cciValue = am.cci(self.cciWindow)
        self.atrValue = am.atr(self.atrWindow)

        if self.trading:
            self.orderOnXminBar(bar)

        # 发出状态更新事件
        self.putEvent()
        self.log.info(u'更新 XminBar {}'.format(self.xminBar.datetime))

    def orderOnXminBar(self, bar):
        """
        在 onXminBar 中的的指标计算和下单逻辑
        :param am:
        :param bar:
        :return:
        """

        # 判断是否要进行交易
        self.updateHands()

        # 当前无仓位，发送开仓委托
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            if self.cciValue > 0:
                self.buy(self.bollUp, self.hands, True)

            elif self.cciValue < 0:
                self.short(self.bollDown, self.hands, True)

        # 持有多头仓位
        elif self.pos > 0:
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
            self.intraTradeLow = bar.low
            self.longStop = self.intraTradeHigh - self.atrValue * self.slMultiplier

            self.sell(self.longStop, abs(self.pos), True)

        # 持有空头仓位
        elif self.pos < 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = min(self.intraTradeLow, bar.low)
            self.shortStop = self.intraTradeLow + self.atrValue * self.slMultiplier

            self.cover(self.shortStop, abs(self.pos), True)

    # ----------------------------------------------------------------------

    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

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
            self.log.warning(
                u'{} -> {}, {}, {}'.format(originCapital, self.capital, round(charge, 2), round(profile, 2)))

        if self.isBackTesting():
            if self.capital <= 0:
                # 回测中爆仓了
                self.capital = 0

        # 下止损单
        self.orderOnXminBar(self.xminBar)

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
        if self.slMultiplier == 0:
            return
        if self.atrValue == 0:
            return

        try:
            minHands = max(0, int(self.capital * self.risk / (self.size * self.atrValue * self.slMultiplier)))
            # minHands = int(self.capital / 10000)
        except:
            raise

        maxHands = max(0, int(
            self.capital * 0.95 / (
                self.size * self.bar.close * self.marginRate)))

        self.hands = min(minHands, maxHands)

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(SvtBollChannelStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic

    def loadCtaDB(self, document=None):
        super(SvtBollChannelStrategy, self).loadCtaDB(document)
        if document:
            for k in self._varList:
                try:
                    setattr(self, k, document[k])
                except KeyError:
                    self.log.warning(u'未保存的key {}'.format(k))
