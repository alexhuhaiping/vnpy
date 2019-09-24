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
from vnpy.trader.vtObject import VtTradeData, VtBarData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class DoubleFilterMAStrategy(CtaTemplate):
    """日均线策略"""
    className = '二重过滤均线策略'
    author = 'lamter'

    fixhands = 1  # 固定手数
    MA_SMALL_D = 15  # 小日均线
    MA_BIG_D = 30  # 大日均线
    TREND = 3  # 取几个K线值来判断趋势
    STOP_PRO = 0.05  # 浮盈达到保证金的 50% 就止盈
    STOP = 0.005  # 止损价位
    DIREC = None  # 多空

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'fixhands',
        'MA_SMALL_D',
        'MA_BIG_D',
        'TREND',
        'STOP_PRO',
        'STOP',
        'DIREC',
    ])

    # 策略变量
    long_tag = True  # 可开多
    short_tag = True  # 可开空
    trend = None  # 当前大趋势
    stop_pro_times = 0

    # 变量列表，保存了变量的名称
    _varList = [
        'long_tag',
        'short_tag',
        'trend',
        'stop_pro_times',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DoubleFilterMAStrategy, self).__init__(ctaEngine, setting)

        self.hands = self.fixhands or 1

        self.ma_sd = None  # 小日均线
        self.ma_bd = None  # 大日均线

        # 要记录的技术指标
        self.techIndLine = {
            'ma_sd': ([], []),
            'ma_bd': ([], []),
        }

        if self.DIREC is not None:
            self.long_tag = not self.DIREC or self.DIREC == DIRECTION_LONG
            self.shortTag = not self.DIREC or self.DIREC == DIRECTION_SHORT
            self.log.info(f'只开 {self.DIREC}')

        # 平仓单
        self.close_stop_profile_order = None  # 止盈单
        self.close_stop_order = None  # 止损单

    def initMaxBarNum(self):
        self.maxBarNum = self.TREND * 2
        self.maxDailyBarNum = self.MA_BIG_D

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog('%s策略初始化' % self.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.maxBarNum)
        initDailyData = self.loadDailyBar(self.MA_BIG_D)

        self.log.info('即将加载 {} 条 bar 数据'.format(len(initData)))

        self.initContract()

        # 从数据库加载策略数据，要在加载 bar 之前。因为数据库中缓存了技术指标
        if not self.isBackTesting():
            # 需要等待保证金加载完毕
            document = self.fromDB()
            self.loadCtaDB(document)

        bar = None
        for bar in initDailyData:
            self.onDailyBar(bar)

        self.bm.preDailyBar = bar

        bar = None
        for bar in initData:
            self.bm.bar = bar
            if not self.isBackTesting():
                if self.tradingDay != bar.tradingDay:
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

        self.update_ma()

        self.orderOpen(self.preXminBar)

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
        # 主要用于回测中更新日线，必须放在第一位
        if self.inited:
            self.bm.updateDailyBar(bar)

        self.bm.updateXminBar(bar)
        if self.isCloseoutVaild and self.rtBalance < 0:
            # 爆仓，一键平仓
            self.closeout()

        if not self.trading:
            return

        # 更新止盈止损价
        self.updateStop(bar)

        # 下止损单
        self.orderClose()

    def onDailyBar(self, dailyBar):
        """
        加载日K线
        :param dailyBar:
        :return:
        """
        bar = dailyBar

        self.am_d.updateBar(bar)
        # 大小日均线
        self.update_ma()

    def update_ma(self):
        """

        :return:
        """
        if np.isnan(self.am.close).all():
            # close 全是 nan 时，无法计算，直接设为 nan
            self.ma_bd = self.ma_bd = np.nan
        else:
            # 正常计算
            self.ma_sd = self.am_d.ma(self.MA_SMALL_D)
            if not np.isnan(self.ma_sd):
                self.ma_sd = self.roundToPriceTick(self.ma_sd)

            self.ma_bd = self.am_d.ma(self.MA_BIG_D)
            if not np.isnan(self.ma_bd):
                self.ma_bd = self.roundToPriceTick(self.ma_bd)

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

        if self.is_ma_not_na():
            self.trend = '多' if self.ma_sd >= self.ma_bd else '空'

        if not am.inited:
            return

        # 当前均线
        self.saveTechIndOnXminBar(bar.datetime)

        # 下开仓单
        self.orderOpen(xminBar)

        # 更新止盈止损价
        self.updateStop(self.xminBar)

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def updateStop(self, bar):
        """
        检查是否需要更新止盈位置
        :return:
        """
        if self.pos == 0:
            return

        if self.pos > 0:
            # 多单中
            # 第几次止盈
            times = self.stop_pro_times + 1
            # 计算当前止盈价格
            stop_pro_price = self.averagePrice * (1 + self.STOP_PRO * times)
            stop_pro_price = self.roundToPriceTick(stop_pro_price)

            if bar.high > stop_pro_price:
                # 要更新价位
                stop_pro_move = self.STOP_PRO * self.stop_pro_times
                stop_price = self.roundToPriceTick(self.averagePrice * (1 + stop_pro_move - self.STOP))

                self.stop_pro_times += 1
                times = self.stop_pro_times + 1
                new_stop_pro_price = self.averagePrice * (1 + self.STOP_PRO * times)
                new_stop_pro_price = self.roundToPriceTick(new_stop_pro_price)
                stop_pro_move = self.STOP_PRO * self.stop_pro_times
                new_stop_price = self.roundToPriceTick(self.averagePrice * (1 + stop_pro_move - self.STOP))
                self.log.info(f'多单 开仓均价 {self.roundToPriceTick(self.averagePrice)} 更新价格 止盈  {stop_pro_price} -> {new_stop_pro_price} 止损 {stop_price} -> {new_stop_price}')

        if self.pos < 0:
            # TODO 空单中
            pass

    # ----------------------------------------------------------------------
    def orderClose(self):
        """
        这种情况下不需要止盈单了
        :return:
        """
        if self.pos > 0:
            # 下止损单
            self.cancelAll()
            stop_pro_move = self.STOP_PRO * self.stop_pro_times
            stop_price = self.roundToPriceTick(self.averagePrice * (1 + stop_pro_move - self.STOP))
            self.sell(stop_price, abs(self.pos), stop=True)

            # 止盈单（不要开止盈单）
            # stop_pro_price = self.roundToPriceTick(self.averagePrice * (1 + self.STOP_PRO))
            # self.sell(stop_pro_price, abs(self.pos), stopProfile=True)

        if self.pos < 0:
            # 下止损单
            self.cancelAll()
            stop_price = self.roundToPriceTick(self.averagePrice * (1 + self.STOP))
            self.cover(stop_price, abs(self.pos), stop=True)

            # 下止盈单
            stop_pro_price = self.roundToPriceTick(self.averagePrice * (1 - self.STOP_PRO))
            self.cover(stop_pro_price, abs(self.pos), stopProfile=True)

    # ----------------------------------------------------------------------
    def orderOpen(self, xminBar):

        if self.pos != 0:
            return

        if self.ma_sd is None or self.ma_bd is None:
            self.log.info(f'没有均线数据 ma_sd {self.ma_sd} ma_bd {self.ma_bd}')
            return

        if np.isnan(self.ma_sd) or np.isnan(self.ma_bd):
            self.log.info(f'没有均线数据 ma_sd {self.ma_sd} ma_bd {self.ma_bd}')
            return

        if self.long_tag and self.ma_sd > self.ma_bd:
            # 多头
            goup = self.am.close[-self.TREND:] > self.am.open[-self.TREND:]
            # 开多
            # self.log.info(f'连阳 {list(zip(self.am.open[-self.TREND - 1:], self.am.close[-self.TREND:]))}')
            if goup.all():
                self.buy(xminBar.close, self.hands, stop=True)

        if self.shortTag and self.ma_sd < self.ma_bd:
            # 空头
            godown = self.am.close[-self.TREND:] < self.am.open[-self.TREND:]
            self.log.info(f'连阴 {list(zip(self.am.open[-self.TREND - 1:], self.am.close[-self.TREND:]))}')
            if godown.all():
                self.short(xminBar.close, self.hands, stop=True)

    def saveTechIndOnXminBar(self, dt):
        """
        保存技术指标
        :return:
        """
        for indName, [dtList, dataList] in list(self.techIndLine.items()):
            data = getattr(self, indName)
            dtList.append(dt)
            dataList.append(self.roundToPriceTick(data))

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

    def is_ma_not_na(self):
        return not np.isnan(self.ma_bd) and not np.isnan(self.ma_sd)

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        originCapital, charge, profile = self._onTrade(trade)

        if trade.offset in OFFSET_CLOSE_LIST:
            # 平仓单, 直接撤单即可
            self.cancelAll()
            if self.pos == 0:
                # 重置止盈次数
                self.stop_pro_times = 0

            # if not self.isBackTesting():
            # self.log.warning(self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile))


        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so: VtStopOrder):
        """停止单推送"""
        self.putEvent()
