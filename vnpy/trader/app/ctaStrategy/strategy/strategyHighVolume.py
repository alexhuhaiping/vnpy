import datetime
import arrow
import pandas as pd
import talib

from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue, exception, logDate
from vnpy.trader.vtObject import VtTradeData
from vnpy.trader.app.ctaStrategy.ctaTemplate import (BarManager, ArrayManager)
from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBase import *

OFFSET_CLOSE_LIST = (OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY)


########################################################################
class HighVolumeStrategy(CtaTemplate):
    """
    - 放量突破策略
    - 计算日成交量的标准差
    - 当标准差超过
    """
    className = 'HighVolumeStrategy'
    name = '放量突破'
    author = 'lamter'

    # 策略参数
    fixhands = 1  # 固定手数
    STD_DAYS = 10  # 标准差统计天数

    # 参数列表，保存了参数的名称
    paramList = CtaTemplate.paramList[:]
    paramList.extend([
        'fixhands',
        'STD_DAYS',
    ])

    # 策略变量
    volume_std = 0  # 成交量标准差
    # 变量列表，保存了变量的名称
    _varList = [
        'volume_std',
    ]
    varList = CtaTemplate.varList[:]
    varList.extend(_varList)

    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(HighVolumeStrategy, self).__init__(ctaEngine, setting)

        # if self.isBackTesting():
        #     self.log.info(u'批量回测，不输出日志')
        #     self.log.propagate = False

        self.hands = self.fixhands or 1
        self.techIndLine = {
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

        if self.bar is None:
            return

        # 计算之前的成交量标准差
        self.volume_std = int(talib.STDDEV(self.am.volume, self.STD_DAYS)[-1])
        self.log.info(f'volume_std {self.volume_std}')

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

        if not am.inited:
            return

        self.saveTechIndOnXminBar(bar.datetime)

        if self.trading:
            pass

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

        # 固定仓位
        if self.fixhands is not None:
            # 有固定手数时直接使用固定手数
            self.hands = min(self.maxHands, self.fixhands)
            return

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """
        响应停止单
        :param so:
        :return:
        """

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
        elif order.status == STATUS_CANCELLED:
            pass

        log('状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))
        # self.log.warning(u'{vtOrderID} 状态:{status} 成交:{tradedVolume}'.format(**order.__dict__))

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        assert isinstance(trade, VtTradeData)

        originCapital, charge, profile = self._onTrade(trade)

        posChange = self.pos - self.prePos

        # 开平仓成本

        # 发出状态更新事件
        self.saveDB()
        self.putEvent()

        # self.printOutOnTrade(trade, OFFSET_CLOSE_LIST, originCapital, charge, profile)

    def toSave(self):
        """
        将策略新增的 varList 全部存库
        :return:
        """
        dic = super(HighVolumeStrategy, self).toSave()
        # 将新增的 varList 全部存库
        dic.update({k: getattr(self, k) for k in self._varList})
        return dic
