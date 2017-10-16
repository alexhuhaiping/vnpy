# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import logging
import copy
from collections import OrderedDict
import pymongo
from itertools import chain

import arrow

from vnpy.trader.vtConstant import *
from vnpy.trader.vtEvent import *
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate as vtCtaTemplate
from vnpy.trader.app.ctaStrategy.ctaTemplate import TargetPosTemplate as vtTargetPosTemplate
from vnpy.trader.vtObject import VtBarData, VtCommissionRate

if __debug__:
    from vnpy.trader.svtEngine import MainEngine
    from vnpy.trader.vtObject import VtContractData


########################################################################
class CtaTemplate(vtCtaTemplate):
    """CTA策略模板"""

    barPeriod = 1  # n 分钟的K线
    barMinute = 1  # K线当前的分钟

    paramList = vtCtaTemplate.paramList[:]
    paramList.extend([
        'barPeriod',
        'barMinute',
        'marginRate',
    ])

    def __init__(self, ctaEngine, setting):
        super(CtaTemplate, self).__init__(ctaEngine, setting)
        loggerName = 'ctabacktesting' if self.isBackTesting() else 'cta'
        logger = logging.getLogger(loggerName)

        # 定制 logger.name
        self.log = logging.getLogger(self.vtSymbol)
        # self.log.parent = logger
        self.log.propagate = 0

        for f in logger.filters:
            self.log.addFilter(f)
        for h in logger.handlers:
            self.log.addHandler(h)

        if self.isBackTesting():
            self.log.setLevel(logger.level)

        # 复制成和原来的 Logger 配置一样

        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None
        self._size = None  # 每手的单位
        self.bar1min = None  # 1min bar
        self.bar = None  # 根据 barPeriod 聚合的 bar
        self.bar1minCount = 0

        if self.isBackTesting():
            # 回测时的资金
            self.balance = self.ctaEngine.capital
        else:
            # TODO 实盘中的资金
            self.balance = 100000

        self._pos = 0
        self.posList = []
        self._marginRate = None
        self.commissionRate = None  # 手续费率 vtObject.VtCommissionRate
        self.marginList = []

        self.registerEvent()

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, pos):
        self._pos = pos
        self.posList.append(pos)

        try:
            margin = self._pos * self.size * self.bar1min.close * self.marginRate
            self.marginList.append(abs(margin / self.balance))
        except AttributeError as e:
            if self.bar1min is None:
                pass
        except TypeError:
            if self.marginRate is None:
                pass

    @property
    def calssName(self):
        return self.__class__.__name__

    def onTrade(self, trade):
        """
        :param trade: VtTradeData
        :return:
        """
        raise NotImplementedError

    def getCharge(self, offset, price):
        # 手续费
        if self.isBackTesting():
            # 回测时使用的是比例手续费
            return self.ctaEngine.rate * self.size * price
        else:
            # TODO 实盘手续费
            return 0

    def charge(self, offset, price, volume):
        """
        扣除手续费
        :return:
        """
        charge = volume * self.getCharge(offset, price)
        self.log.info(u'手续费 {}'.format(charge))
        self.balance -= charge

    def chargeSplipage(self, volume):
        slippage = volume * self.size * self.ctaEngine.slippage
        self.balance -= slippage
        self.log.info(u'滑点 {}'.format(slippage))

    def newBar(self, tick):
        bar = VtBarData()
        bar.vtSymbol = tick.vtSymbol
        bar.symbol = tick.symbol
        bar.exchange = tick.exchange

        bar.open = tick.lastPrice
        bar.high = tick.lastPrice
        bar.low = tick.lastPrice
        bar.close = tick.lastPrice

        bar.date = tick.date
        bar.time = tick.time
        bar.datetime = tick.datetime  # K线的时间设为第一个Tick的时间

        # 实盘中用不到的数据可以选择不算，从而加快速度
        bar.volume = tick.volume
        bar.openInterest = tick.openInterest
        return bar

    def refreshBarByTick(self, bar, tick):
        bar.high = max(bar.high, tick.lastPrice)
        bar.low = min(bar.low, tick.lastPrice)
        bar.close = tick.lastPrice

    def refreshBarByBar(self, bar, bar1min):
        bar.high = max(bar.high, bar1min.high)
        bar.low = min(bar.low, bar1min.low)
        bar.close = bar1min.close

    def paramList2Html(self):
        return OrderedDict(
            (k, getattr(self, k)) for k in self.paramList
        )

    def varList2Html(self):
        return OrderedDict(
            (k, getattr(self, k)) for k in self.varList
        )

    def toHtml(self):
        items = (
            ('param', self.paramList2Html()),
            ('var', self.varList2Html()),
        )
        orderDic = OrderedDict(items)
        orderDic['bar{}Min'.format(self.barPeriod)] = self.barToHtml()
        orderDic['bar1min'] = self.bar1minToHtml()
        return orderDic

    def loadBar(self, barNum):
        """加载用于初始化策略的数据"""
        return self.ctaEngine.loadBar(self.vtSymbol, self.barCollection, barNum, self.barPeriod)

    @property
    def mainEngine(self):
        assert isinstance(self.ctaEngine.mainEngine, MainEngine)
        return self.ctaEngine.mainEngine

    @property
    def contract(self):
        """
        合约
        :return:
        """
        if self.isBackTesting():
            return '111'
        else:
            contract = self.mainEngine.getContract(self.vtSymbol)
            assert isinstance(contract, VtContractData) or contract is None
            return contract

    def isBackTesting(self):
        return self.getEngineType() == ENGINETYPE_BACKTESTING

    @property
    def priceTick(self):
        if self._priceTick is None:
            if self.isBackTesting():
                # 回测中
                self._priceTick = self.ctaEngine.priceTick
            else:
                # 实盘
                self._priceTick = self.contract.priceTick

        assert isinstance(self._priceTick, float) or isinstance(self._priceTick, int)
        return self._priceTick

    @property
    def marginRate(self):
        if self._marginRate is None:
            if self.isBackTesting():
                # 回测中
                self._marginRate = self.ctaEngine.marginRate
                # else:
                #     # 实盘 默认设置为 10%
                #     # self._marginRate = self.contract.marginRate
                #     self._marginRate = None

        assert isinstance(self._marginRate, float) or isinstance(self._marginRate, int) or self._marginRate is None
        return self._marginRate

    def getCommission(self, price, volume, offset):
        """

        :param price:
        :param volume:
        :param offset:
        :return:
        """

        if self.isBackTesting():
            # 回测中
            return price * volume * self.ctaEngine.rate
        else:

            assert isinstance(self.commissionRate, VtCommissionRate)
            m = self.commissionRate
            if offset == OFFSET_OPEN:
                # 开仓
                # 直接将两种手续费计费方式累加
                value = m.openRatioByMoney * price * volume
                value += m.openRatioByVolume * volume
            elif offset == OFFSET_CLOSE:
                # 平仓
                value = m.closeRatioByMoney * price * volume
                value += m.closeRatioByVolume * volume
            elif offset == OFFSET_CLOSEYESTERDAY:
                # 平昨
                value = m.closeRatioByMoney * price * volume
                value += m.closeRatioByVolume * volume
            elif offset == OFFSET_CLOSETODAY:
                # 平今
                value = m.closeTodayRatioByMoney * price * volume
                value += m.closeTodayRatioByVolume * volume
            else:
                err = u'未知的开平方向 {}'.format(offset)
                self.log.error(err)
                raise ValueError(err)
            return value

    @property
    def size(self):
        if self._size is None:
            if self.isBackTesting():
                # 回测中
                self._size = self.ctaEngine.size
            else:
                # 实盘
                self._size = self.contract.size

        assert isinstance(self._size, float) or isinstance(self._size, int)
        return self._size

    def onBar(self, bar1min):
        if self.isBackTesting():
            self.bar1min = bar1min

        if self.bar is None:
            # 还没有任何数据
            self.bar = copy.copy(bar1min)
        elif self.isNewBar():
            # bar1min 已经凑齐了一个完整的 bar
            self.bar = copy.copy(bar1min)
        else:
            # 还没凑齐一个完整的 bar
            self.refreshBarByBar(self.bar, bar1min)

        self.bar1minCount += 1

    def isNewBar(self):
        return self.bar1minCount % self.barPeriod == 0

    def stop(self):
        # 执行停止策略
        self.ctaEngine.stopStrategy(self)

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        # 必要的三个字段
        dic = self.filterSql()
        dic['datetime'] = arrow.now().datetime,
        return dic

    def saveDB(self):
        """
        将策略的数据保存到 mongodb 数据库
        :return:
        """
        if self.isBackTesting():
            # 回测中，不存库
            return

        # 保存
        document = self.toSave()
        self.ctaEngine.saveCtaDB(self.filterSql(), {'$set': document})

    def filterSql(self):
        gateWay = self.mainEngine.getGateway('CTP')
        return {
            'symbol': self.vtSymbol,
            'className': self.className,
            'userID': gateWay.tdApi.userID,
        }
    def fromDB(self):
        """

        :return:
        """
        # 对 datetime 倒叙，获取第一条
        return self.ctaEngine.ctaCol.find_one(self.filterSql(), sort=[('datetime', pymongo.DESCENDING)])

    def onOrder(self, order):
        """
        order.direction
            # 方向常量
            DIRECTION_NONE = u'无方向'
            DIRECTION_LONG = u'多'
            DIRECTION_SHORT = u'空'
            DIRECTION_UNKNOWN = u'未知'
            DIRECTION_NET = u'净'
            DIRECTION_SELL = u'卖出'              # IB接口
            DIRECTION_COVEREDSHORT = u'备兑空'    # 证券期权

        order.offset
            # 开平常量
            OFFSET_NONE = u'无开平'
            OFFSET_OPEN = u'开仓'
            OFFSET_CLOSE = u'平仓'
            OFFSET_CLOSETODAY = u'平今'
            OFFSET_CLOSEYESTERDAY = u'平昨'
            OFFSET_UNKNOWN = u'未知'
        :param order:
        :return:
        """

        raise NotImplementedError

    def roundToPriceTick(self, price):
        """取整价格到合约最小价格变动"""
        if not self.priceTick:
            return price

        newPrice = round(price / self.priceTick, 0) * self.priceTick
        return newPrice

    def barToHtml(self):
        if self.bar is None:
            return u'bar 无数据'
        itmes = (
            ('datetime', self.bar.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', self.bar.open,),
            ('high', self.bar.high),
            ('low', self.bar.low),
            ('close', self.bar.close),
        )
        return OrderedDict(itmes)

    def bar1minToHtml(self):
        if self.bar1min is None:
            return u'bar1min 无数据'
        itmes = (
            ('datetime', self.bar1min.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', self.bar1min.open,),
            ('high', self.bar1min.high),
            ('low', self.bar1min.low),
            ('close', self.bar1min.close),
        )
        return OrderedDict(itmes)

    def registerEvent(self):
        """注册事件监听"""
        if self.isBackTesting():
            # 回测中不注册监听事件
            return
        en = self.ctaEngine.mainEngine.eventEngine
        en.register(EVENT_MARGIN_RATE, self.updateMarginRate)
        en.register(EVENT_COMMISSION_RATE, self.updateCommissionRate)

    def updateMarginRate(self, event):
        """更新合约数据"""
        marginRate = event.dict_['data']
        if marginRate.vtSymbol != self.vtSymbol:
            return

        self._marginRate = marginRate.rate

    def updateCommissionRate(self, event):
        """更新合约数据"""
        commissionRate = event.dict_['data']

        # commissionRate.vtSymbol 可能为 'rb' 或者 'rb1801' 前者说明合约没改过，后者说明该合约有变动
        if commissionRate.underlyingSymbol == self.vtSymbol:
            # 返回 rb1801, 合约有变动，强制更新
            self.commissionRate = commissionRate
            return
        elif self.vtSymbol.startswith(commissionRate.underlyingSymbol):
            # 返回 rb ,合约没有变动
            self.commissionRate = commissionRate
            return
        else:
            pass


########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    def onBar(self, bar1min):
        vtTargetPosTemplate.onBar(self, bar1min)
        CtaTemplate.onBar(self, bar1min)
