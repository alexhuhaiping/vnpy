import datetime
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
class DonchianContrarianStrategy(CtaTemplate):
    """唐奇安反转
    - 当k0.close < 上轨 and 上轨 - k0.close < atr 时进入下单状态
        - 下单点位为 上轨 - atr
        - 当 k1.high > 上轨，撤单等待
        - 当 k1.high > 上轨，止损
        - 止损点位 下轨 + atr
    """
    className = 'DonchianContrarianStrategy'
    name = '唐奇安通道反转'
    author = 'lamter'

    # 策略参数
    CHANNEL = 20  # 唐奇安通道周期
    ATR_N = 14  # ATR 长度
    STOP_BAR = 3 # 3根K线后主动止盈
    fixhands = 1  # 固定手数

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'CHANNEL',
        'ATR_N',
        'STOP_BAR',
        'fixhands',
    ])

    # 策略变量
    atr = None  # atr 值
    up = None  # 上轨
    down = None  # 下轨
    long_ready = False  # 准备开多
    short_ready = False  # 准备开空

    # 变量列表，保存了变量的名称
    _varList = [
        'atr',
        'up',
        'down',
        'long_ready',
        'short_ready',

    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DonchianContrarianStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands or 1
        self.techIndLine = {
            'up': ([], []), 'down': ([], []),
        }

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

        # 撤所有的单
        self.cancelAll()

        # # 开盘前再下单
        # self.orderUntilTradingTime()

        self.putEvent()

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

        # 计算ATR
        self.atr = self.roundToPriceTick(am.atr(self.ATR_N))
        # 计算唐奇安指标
        self.up, self.down = am.donchian(self.CHANNEL)

        if not am.inited:
            return

        self.saveTechIndOnXminBar(bar.datetime)

        if not self.trading:
            return

        # 计算逼近临界点
        long_edge = self.up - self.atr
        short_edge = self.down + self.atr
        up_array, down_array = am.donchian(self.CHANNEL, True)

        # 判断临界点是否正常
        if long_edge - short_edge > self.atr * 2:
            # 上下临界点要错开至少1个atr

            if xminBar.close > long_edge and xminBar.high < self.up and up_array[-1] <= up_array[-4]:
                self.short_ready = True

            if xminBar.close < short_edge and xminBar.low > self.down and down_array[-1] > down_array[-4]:
                self.long_ready = True

        if self.pos == 0:
            self.updateHands()
            if self.short_ready:
                # 上轨反转，下空单
                self.log.info(f'下开空单 {long_edge}')
                self.short(long_edge, self.hands, stop=True)
            elif self.long_ready:
                # 下轨反转，下多单
                self.log.info(f'下开多单 {short_edge}')
                self.buy(short_edge, self.hands, stop=True)
            else:
                # 撤掉所有单
                self.cancelAll()
        elif self.pos > 0:
            # 多单
            self.orderCloseLong()
        elif self.pos < 0:
            # 空单
            self.orderCloseShort()

        self.short_ready = self.long_ready = False

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()


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
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        log = self.log.info
        message = ''
        if order.status == STATUS_REJECTED:
            log = self.log.warning
            for k, v in list(order.rawData.items()):
                message += '{}:{}\n'.format(k, v)
            log(message)
        elif order.status == STATUS_CANCELLED:
            pass

        log('状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))
        # self.log.warning(u'{vtOrderID} 状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))

    # ----------------------------------------------------------------------
    def onTrade(self, trade: VtTradeData):

        originCapital, charge, profile = self._onTrade(trade)
        pos = self.pos - self.prePos
        if pos < 0:
            # 空单
            self.orderCloseShort()
        if pos > 0:
            # 多单
            self.orderCloseLong()

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

        # self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile)

    def orderCloseShort(self):
        self.cancelAll()
        #  下止损单，触碰上轨止损
        self.cover(self.up, abs(self.pos), stop=True)
        #  下止盈单，触碰下轨临界点止盈
        self.cover(self.down + self.atr, abs(self.pos), stopProfile=True)

    def orderCloseLong(self):
        self.cancelAll()
        #  下止损单，触碰下轨止损
        self.sell(self.down, abs(self.pos), stop=True)
        #  下止盈单，触碰上轨临界点止盈
        self.sell(self.up - self.atr, abs(self.pos), stopProfile=True)

    def onStopOrder(self, so):
        pass

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(DonchianContrarianStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic
