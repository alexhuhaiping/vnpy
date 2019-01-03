# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

import copy
import traceback
import time
from threading import Timer, Event
import talib
import logging
import pymongo
import datetime

import pandas as pd
import arrow
import tradingtime as tt

from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import waitToContinue
from vnpy.trader.vtEvent import *
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate as vtCtaTemplate
from vnpy.trader.app.ctaStrategy.ctaTemplate import ArrayManager as VtArrayManager
from vnpy.trader.app.ctaStrategy.ctaTemplate import BarManager as VtBarManager
from vnpy.trader.app.ctaStrategy.ctaTemplate import TargetPosTemplate as vtTargetPosTemplate
from vnpy.trader.vtObject import VtBarData, VtCommissionRate, VtTradeData

if __debug__:
    from vnpy.trader.svtEngine import MainEngine
    from vnpy.trader.vtEngine import DataEngine
    from vnpy.trader.vtObject import VtContractData


########################################################################
class CtaTemplate(vtCtaTemplate):
    """CTA策略模板"""

    barXmin = 2  # n 分钟的K线
    overSplipage = 2  # 滑点过大

    # 默认初始资金是1万, 在 onTrade 中平仓时计算其盈亏
    # 有存库的时候使用存库中的 capital 值，否则使用 CTA_setting.json 中的值
    capital = 10000

    paramList = vtCtaTemplate.paramList[:]
    paramList.extend([
        'barXmin',
        'marginRate',
        'capital',
    ])

    _varList = []

    varList = vtCtaTemplate.varList[:]
    varList.extend(_varList)

    # 权益情况
    BALANCE = [
        'capital',
        'turnover',
        'averagePrice',
        'floatProfile',
        'rtBalance',
        'marginRatio',
    ]

    # 成交状态
    TRADE_STATUS_OPEN_LONG = u'开多'  # 开多
    TRADE_STATUS_CLOSE_LONG = u'平多'  # 平多
    TRADE_STATUS_DEC_LONG = u'减多'  # 减多
    TRADE_STATUS_INC_LONG = u'加多'  # # 加多
    TRADE_STATUS_REV_LONG = u'反多'  # 反多

    TRADE_STATUS_OPEN_SHORT = u'开空'  # 开空
    TRADE_STATUS_CLOSE_SHORT = u'平空'  # 平空
    TRADE_STATUS_DEC_SHORT = u'减空'  # 减空
    TRADE_STATUS_INC_SHORT = u'加空'  # # 加空
    TRADE_STATUS_REV_SHORT = u'反空'  # 反空

    def __str__(self):
        s = super(CtaTemplate, self).__str__()
        return s.replace(self.__class__.__name__, self.__class__.__name__ + '.{}'.format(self.vtSymbol))

    def __init__(self, ctaEngine, setting):
        super(CtaTemplate, self).__init__(ctaEngine, setting)
        self.log = logging.getLogger(self.name + '_' + self.vtSymbol)
        self._setting = setting.copy()

        if not isinstance(self.barXmin, int):
            raise ValueError(u'barXmin should be int.')

        self.stopOrdering = Event()  # 停止单锁定
        self.stopOrderingCount = 0  # 停止单锁定计数
        self.saving = False  # 是否可以存库了
        self.barCollection = MINUTE_COL_NAME  # MINUTE_COL_NAME OR DAY_COL_NAME
        self._priceTick = None
        self._size = None  # 每手的单位
        self.balanceList = OrderedDict()

        self._pos = 0
        self.posList = []
        self._marginRate = None
        self.commissionRate = None  # 手续费率 vtObject.VtCommissionRate
        self.marginList = []
        self._positionDetail = None  # 仓位详情

        self._commisionAmonut = 0  # 回测统计用的手续费总数
        self._splipageAmonut = 0  # 回测统计用的滑点总数

        self.tradingDay = None  # 当前所处的交易日

        # K线管理器
        self.maxBarNum = 0
        self.initMaxBarNum()
        self.bm = BarManager(self, self.onBar, self.barXmin, self.onXminBar)  # 创建K线合成器对象
        # 技术指标生成器
        self.am = ArrayManager(self.maxBarNum)

        # 是否允许一键平仓
        self.isCloseoutVaild = False

        # 计算持仓成本
        self.turnover = EMPTY_FLOAT  # 持仓总值，多空正负
        # self.avrPrice = EMPTY_FLOAT  # 持仓均价，多空正负

        self.registerEvent()
        self.prePos = self._pos

        self.isNeedUpdateMarginRate = True
        self.isNeedUpdateCommissionRate = True

        self.winCount = 0  # 连胜计数
        self.loseCount = 0  # 连败计数
        self.slight = False  # 轻重仓标记

    @property
    def floatProfile(self):
        if self.pos == 0:
            return 0
        tick = self.bm.lastTick
        if tick:
            if self.pos > 0:
                return (tick.bidPrice1 - self.averagePrice) * self.pos * self.size
            if self.pos < 0:
                return (tick.askPrice1 - self.averagePrice) * self.pos * self.size

        if not self.bar:
            return 0

        return (self.bar.close - self.averagePrice) * self.pos * self.size

    @property
    def rtBalance(self):
        return self.capital + self.floatProfile

    @property
    def marginRatio(self):
        try:
            return abs(round(self.turnover * self.marginRate / self.rtBalance, 2))
        except ZeroDivisionError:
            return 0

    @property
    def averagePrice(self):
        try:
            return self.turnover / self.pos / self.size
        except ZeroDivisionError:
            return 0

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, pos):
        self.prePos, self._pos = self._pos, pos

        if self.inited and self.trading and self.isBackTesting():
            self.posList.append(pos)
            try:
                margin = self._pos * self.size * self.bar.close * self.marginRate
                self.marginList.append(abs(margin / self.capital))
            except AttributeError as e:
                if self.bar is None:
                    pass
                else:
                    raise
            except TypeError:
                if self.marginRate is None:
                    pass
            except ZeroDivisionError:
                # 可用资金为0
                self.marginList.append(1)

    @property
    def bar(self):
        return self.bm.bar

    @property
    def xminBar(self):
        return self.bm.xminBar

    @property
    def preBar(self):
        return self.bm.preBar

    @property
    def preXminBar(self):
        return self.bm.preXminBar

    @property
    def calssName(self):
        return self.__class__.__name__

    def onStart(self):
        if not self.isCloseoutVaild:
            raise ValueError(u'未设置平仓标记位 isCloseoutVaild')
        super(CtaTemplate, self).onStart()

    def buy(self, price, volume, stop=False, stopProfile=False):
        """买开"""
        if stopProfile:
            # 止盈停止单
            return self.sendOrder(CTAORDER_BUY, price, volume, stop, stopProfile)
        else:
            # 其余单子
            return super(CtaTemplate, self).buy(price, volume, stop)

    def sell(self, price, volume, stop=False, stopProfile=False):
        """卖平"""
        if stopProfile:
            # 止盈停止单
            return self.sendOrder(CTAORDER_SELL, price, volume, stop, stopProfile)
        else:
            # 其余单子
            return super(CtaTemplate, self).sell(price, volume, stop)

    def short(self, price, volume, stop=False, stopProfile=False):
        """卖开"""
        if stopProfile:
            # 止盈停止单
            return self.sendOrder(CTAORDER_SHORT, price, volume, stop, stopProfile)
        else:
            # 其余单子
            return super(CtaTemplate, self).short(price, volume, stop)

    def cover(self, price, volume, stop=False, stopProfile=False):
        if stopProfile:
            # 止盈停止单
            return self.sendOrder(CTAORDER_COVER, price, volume, stop, stopProfile)
        else:
            # 其余单子
            return super(CtaTemplate, self).cover(price, volume, stop)

    def sendOrder(self, orderType, price, volume, stop=False, stopProfile=False):
        if stop == stopProfile == True:
            raise ValueError(u'不能同时设置停止单为止盈和止损!')

        # self.log.warning(u'{} {} {} {}'.format(orderType, self.ctaEngine.roundToPriceTick(price), volume, stop))
        if stopProfile:
            vtOrderIDs = self.ctaEngine.sendStopOrder(self.vtSymbol, orderType, price, volume, self, stopProfile)
        else:
            vtOrderIDs = super(CtaTemplate, self).sendOrder(orderType, price, volume, stop)

        # # 下单后保存策略数据
        # self.saveDB()

        return vtOrderIDs

    #     """
    #     保存成交单
    #     :return:
    #     """
    #     trade = event.dict_['data']
    #     assert isinstance(trade, VtTradeData)
    #
    #     if trade.vtSymbol != self.vtSymbol:
    #         return
    #
    #     self.log.info(u'保存成交单 {}'.format(trade.tradeID))
    #     dic = trade.__dict__.copy()
    #     dic.pop('rawData')
    #
    #     # 时间戳
    #     dt = dic['datetime']
    #
    #     if not dt.tzinfo:
    #         t = u'成交单 {} {} 没有时区'.format(trade.symbol, dt)
    #         raise ValueError(t)
    #     td = dic['tradingDay']
    #     if td is None:
    #         t = u'成交单 {} {} 没有交易日'.format(trade.symbol, dt)
    #         raise ValueError(t)
    #     dic['class'] = self.className
    #     dic['name'] = self.name
    #     dic['pos'] = self.pos
    #     dic.update(self.positionDetail.toHtml())
    #
    #     self.ctaEngine.saveTrade(dic)

    def capitalBalance(self, trade):
        """
        计算持仓成本和利润
        支持锁仓模式
        :param trade:
        :return:
        """
        # 计算之前的持仓，多头为正，空头为负
        volume = trade.volume if trade.direction == DIRECTION_LONG else -trade.volume
        prePos = self.pos - volume

        status = self.tradeStatsu(prePos, self.pos)

        profile = 0
        if status in (self.TRADE_STATUS_OPEN_LONG, self.TRADE_STATUS_OPEN_SHORT):
            # 开多,开空
            self.turnover = volume * trade.price * self.size
        elif status in (self.TRADE_STATUS_INC_LONG, self.TRADE_STATUS_INC_SHORT):
            # 加多, 加空
            self.turnover += volume * trade.price * self.size
        elif status == self.TRADE_STATUS_CLOSE_LONG:
            # 平多
            profile = abs(trade.price * volume * self.size) - abs(self.turnover)
            self.turnover = 0
        elif status == self.TRADE_STATUS_CLOSE_SHORT:
            # 平空
            profile = abs(self.turnover) - abs(trade.price * volume * self.size)
            self.turnover = 0

        elif status == self.TRADE_STATUS_DEC_LONG:
            # 减多
            price = self.turnover / (prePos * self.size)
            turnover = price * volume * self.size
            profile = abs(trade.price * volume * self.size) - abs(turnover)
            self.turnover = price * self.pos * self.size
        elif status == self.TRADE_STATUS_DEC_SHORT:
            # 减空
            price = self.turnover / (prePos * self.size)
            turnover = price * volume * self.size
            profile = abs(turnover) - abs(trade.price * volume * self.size)
            self.turnover = price * self.pos * self.size
        elif status == self.TRADE_STATUS_REV_LONG:
            # 反多
            profile = abs(self.turnover) - abs(trade.price * prePos * self.size)
            # 开多
            self.turnover = self.pos * trade.price * self.size
        elif status == self.TRADE_STATUS_REV_SHORT:
            # 反空
            profile = abs(trade.price * prePos * self.size) - abs(self.turnover)
            self.turnover = 0
            # 开空
            self.turnover += self.pos * trade.price * self.size
        else:
            raise ValueError(u'未知的状态 {}'.format(status))

        self.capital += profile

    def charge(self, offset, price, volume):
        """
        扣除手续费
        :return:
        """
        commission = self.getCommission(price, volume, offset)
        self.log.info(u'手续费 {}'.format(commission))
        self.capital -= commission
        self._commisionAmonut += commission

    def chargeSplipage(self, volume):
        """
        回测时的滑点
        :param volume:
        :return:
        """
        if self.isBackTesting():
            slippage = volume * self.size * self.ctaEngine.slippage
            self.capital -= slippage
            self._splipageAmonut += slippage

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
            ((k, getattr(self, k)) for k in self.paramList)
        )

    def varList2Html(self):
        return OrderedDict(
            ((k, getattr(self, k)) for k in self.varList)
        )

    def balance2Html(self):
        return OrderedDict(
            ((k, getattr(self, k)) for k in self.BALANCE)
        )

    def toHtml(self):
        try:
            param = self.paramList2Html()
            var = self.varList2Html()
            balance = self.balance2Html()
            items = (
                ('balance', balance),
                ('param', param),
                ('var', var),
            )

            orderDic = OrderedDict(items)

            if self.preBar:
                orderDic['preBar'] = self.barToHtml(self.preBar)
            orderDic['bar'] = self.barToHtml()

            if self.preXminBar:
                orderDic['pre{}minBar'.format(self.barXmin)] = self.xminBarToHtml(self.preXminBar)
            orderDic['{}minBar'.format(self.barXmin)] = self.xminBarToHtml()

            # 限价单 orders 里面是 odic，包含限价单的内容
            orders = self.ctaEngine.getAllOrderToShow(self.name)
            orderDic['order'] = pd.DataFrame(orders).to_html()

            # 本地停止单
            stopOrders = self.ctaEngine.getAllStopOrderToShow(self.name)
            stopOrders.sort(key=lambda s: (s.direction, s.stopProfile))
            units = [so.toHtml(self.bar) for so in stopOrders]
            orderDic['stopOrder'] = pd.DataFrame(units).to_html()

            # 持仓详情
            orderDic['posdetail'] = self.positionDetail.toHtml()
            return orderDic
        except:
            err = traceback.format_exc()
            self.log.error(err)

        return {}

    @property
    def positionDetail(self):
        if self._positionDetail is None:
            self._positionDetail = self.dataEngine.getPositionDetail(self.vtSymbol)
        return self._positionDetail

    def loadBar(self, barNum):
        """加载用于初始化策略的数据"""
        return self.ctaEngine.loadBar(self.vtSymbol, self.barCollection, barNum + 1, self.barXmin)

    @property
    def mainEngine(self):
        assert isinstance(self.ctaEngine.mainEngine, MainEngine)
        return self.ctaEngine.mainEngine

    @property
    def dataEngine(self):
        assert isinstance(self.ctaEngine.mainEngine.dataEngine, DataEngine)
        return self.ctaEngine.mainEngine.dataEngine

    @property
    def contract(self):
        """
        合约
        :return:
        """
        if self.isBackTesting():
            return self.ctaEngine.vtContract
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
        try:
            return self._marginRate.marginRate
        except AttributeError:
            if self.isBackTesting():
                # 回测中
                self._marginRate = self.ctaEngine.marginRate
                return self.marginRate
            else:
                return 0.9

    def getCommission(self, price, volume, offset):
        """

        :param price:
        :param volume:
        :param offset:
        :return:
        """

        if self.isBackTesting():
            # 回测中
            m = self.ctaEngine.rate
        else:

            m = self.commissionRate

        assert isinstance(m, VtCommissionRate)

        turnover = price * volume * self.size
        if offset == OFFSET_OPEN:
            # 开仓
            # 直接将两种手续费计费方式累加
            value = m.openRatioByMoney * turnover
            value += m.openRatioByVolume * volume
        elif offset == OFFSET_CLOSE:
            # 平仓
            value = m.closeRatioByMoney * turnover
            value += m.closeRatioByVolume * volume
        elif offset == OFFSET_CLOSEYESTERDAY:
            # 平昨
            value = m.closeRatioByMoney * turnover
            value += m.closeRatioByVolume * volume
        elif offset == OFFSET_CLOSETODAY:
            # 平今
            value = m.closeTodayRatioByMoney * turnover
            value += m.closeTodayRatioByVolume * volume
        else:
            err = u'未知的开平方向 {}'.format(offset)
            self.log.error(err)
            raise ValueError(err)
        return round(value, 2)

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

    def onXminBar(self, xminBar):
        raise NotImplementedError(u'尚未定义')

        # def onBar(self, bar1min):
        # if self.isBackTesting():
        #     self.bar1min = bar1min
        #
        # if self.bar is None:
        #     # 还没有任何数据
        #     self.bar = copy.copy(bar1min)
        # elif self.isNewBar():
        #     # bar1min 已经凑齐了一个完整的 bar
        #     self.bar = copy.copy(bar1min)
        # else:
        #     # 还没凑齐一个完整的 bar
        #     self.refreshBarByBar(self.bar, bar1min)
        #
        # self.bar1minCount += 1

    # def isNewBar(self):
    #     return self.bar1minCount % self.barXmin == 0 and self.bar1minCount != 0

    def getOrder(self, vtOrderID):
        if self.isBackTesting():
            # 回测模式中取回订单对象
            return self.ctaEngine.getOrder(vtOrderID)
        else:
            # 实盘中取回订单对象
            return self.mainEngine.getOrder(vtOrderID)

    def loadCtaDB(self, document):
        if not document:
            return
        self.capital = document['capital']
        self.turnover = document['turnover']

        # orderIDs = document.get('orderIDs')
        #
        # tradingDay = tt.get_tradingday(arrow.now().datetime)[1]
        #
        # if orderIDs and tradingDay == document['tradingDay']:
        #     # 同一交易日，加载缓存的订单ID
        #     _orderIDs = self.ctaEngine.strategyOrderDict.get(self.name)
        #     if _orderIDs is None:
        #         self.ctaEngine.strategyOrderDict[self.name] = set(orderIDs)
        #     else:
        #         _orderIDs |= set(orderIDs)
        #         self.ctaEngine.strategyOrderDict[self.name] = _orderIDs

    def toSave(self):
        """
        要存库的数据
        :return: {}
        """
        dic = self.filterSql()

        dic['datetime'] = arrow.now().datetime
        dic['tradingDay'] = self.tradingDay or tt.get_tradingday(dic['datetime'])[1]
        dic['capital'] = self.capital
        dic['turnover'] = self.turnover
        dic['rtBalance'] = self.rtBalance

        # orderIDs = self.ctaEngine.strategyOrderDict.get(self.name)
        # if orderIDs is None:
        #     orderIDs = []
        # else:
        #     orderIDs = list(orderIDs)
        # dic['orderIDs'] = orderIDs

        return dic

    def saveDB(self):
        """
        将策略的数据保存到 mongodb 数据库
        :return:
        """

        if self.saving:
            self.log.info(u'保存策略数据')

            # 保存
            document = self.toSave()
            try:
                self.ctaEngine.saveCtaDB(self.filterSql(), {'$set': document})
            except Exception:
                self.log.error(str(document))
                raise

    def filterSql(self):
        gateWay = self.mainEngine.getGateway('CTP')
        return {
            'symbol': self.vtSymbol,
            'className': self.className,
            'userID': gateWay.tdApi.userID,
            'name': self.name,
        }

    def fromDB(self):
        """

        :return:
        """
        # 对 datetime 倒叙，获取第一条
        return self.ctaEngine.ctaCol.find_one(self.filterSql(), sort=[('datetime', pymongo.DESCENDING)])

    def _loadVar(self, document):
        if document:
            for k in self._varList:
                try:
                    setattr(self, k, document[k])
                except KeyError:
                    self.log.warning(u'未保存的key {}'.format(k))

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

    def barToHtml(self, bar=None):
        bar = bar or self.bm.bar
        assert isinstance(bar, VtBarData)
        if bar is None:
            return u'bar 无数据'
        itmes = (
            ('datetime', bar.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', bar.open,),
            ('high', bar.high),
            ('low', bar.low),
            ('close', bar.close),
        )
        return OrderedDict(itmes)

    def xminBarToHtml(self, bar=None):
        bar = bar or self.bm.xminBar
        assert isinstance(bar, VtBarData)
        if bar is None:
            return u'xminBar 无数据'

        itmes = (
            ('datetime', bar.datetime.strftime('%Y-%m-%d %H:%M:%S'),),
            ('open', bar.open,),
            ('high', bar.high),
            ('low', bar.low),
            ('close', bar.close),
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
        # en.register(EVENT_TRADE, self.saveTrade)

    def updateMarginRate(self, event):
        """更新合约数据"""
        marginRate = event.dict_['data']

        if marginRate.vtSymbol != self.vtSymbol:
            return

        self.log.info(u'更新保证金 {}'.format(marginRate.marginRate))

        self.isNeedUpdateMarginRate = False

        self.setMarginRate(marginRate)

    def setMarginRate(self, marginRate):
        self._marginRate = marginRate

    def updateCommissionRate(self, event):
        """更新合约数据"""
        commissionRate = event.dict_['data']

        # commissionRate.vtSymbol 可能为 'rb' 或者 'rb1801' 前者说明合约没改过，后者说明该合约有变动
        if commissionRate.underlyingSymbol == self.vtSymbol:
            # 返回 rb1801, 合约有变动，强制更新
            self.setCommissionRate(commissionRate)
            self.isNeedUpdateCommissionRate = False
            log = u'更新手续费 '
            for k, v in commissionRate.__dict__.items():
                if k == 'rawData':
                    continue
                log += u'{}:{} '.format(k, v)
                self.log.info(log)
            return
        elif self.vtSymbol.startswith(commissionRate.underlyingSymbol):
            # 返回 rb ,合约没有变动
            self.isNeedUpdateCommissionRate = False
            return
        else:
            pass

    def setCommissionRate(self, commissionRate):
        self.commissionRate = commissionRate

    def initContract(self):
        """
        初始化订阅合约
        :return:
        """
        waitContractSeconds = 0
        while self.contract is None:
            waitContractSeconds += 1
            if waitContractSeconds > 10:
                self.inited = False
                self.log.error(u'策略未能订阅合约 {}'.format(self.vtSymbol))
                return
            self.log.info(u'等待合约 {}'.format(self.vtSymbol))
            time.sleep(1)
        else:
            self.log.info(u'订阅合约 {} 成功'.format(self.vtSymbol))

    def initMaxBarNum(self):
        """
        初始化最大 bar 数
        :return:
        """
        self.maxBarNum = 0
        raise NotImplementedError(u'')

    def tradeStatsu(self, prePos, pos):
        """

        :param prePos:
        :param pos:
        :return:
        """

        if prePos == 0 and pos > 0:
            # 开多
            return self.TRADE_STATUS_OPEN_LONG
        if pos == 0 and prePos > 0:
            # 平多
            return self.TRADE_STATUS_CLOSE_LONG
        if prePos > 0 and pos > 0 and prePos > pos:
            # 减多
            return self.TRADE_STATUS_DEC_LONG
        if prePos > 0 and pos > 0 and prePos < pos:
            # 加多
            return self.TRADE_STATUS_INC_LONG
        if prePos > 0 and pos > 0 and prePos < pos:
            # 反多
            return self.TRADE_STATUS_REV_LONG

        if prePos == 0 and pos < 0:
            # 开空
            return self.TRADE_STATUS_OPEN_SHORT
        if pos == 0 and prePos < 0:
            # 平空
            return self.TRADE_STATUS_CLOSE_SHORT
        if prePos < 0 and pos < 0 and abs(pos) < abs(prePos):
            # 减空
            return self.TRADE_STATUS_DEC_SHORT
        if prePos < 0 and pos < 0 and abs(pos) > abs(prePos):
            # 加空
            return self.TRADE_STATUS_INC_SHORT
        if prePos < 0 and pos == 0:
            # 反空
            return self.TRADE_STATUS_REV_SHORT

        self.log.warning(u'prePos:{} pos:{}'.format(prePos, pos))
        return None

    def closeout(self):
        """
        一键平仓
        :return:
        """
        if not self.isCloseoutVaild:
            raise ValueError(u'未设置可强平')

        if self.pos == 0:
            # 无需一键平仓
            return

        if self.bm.lastTick or self.bar:
            pass
        else:
            self.log.warning(u'没有 tick 或  bar 能提供价格一键平仓')
            return

        t = u'\n'.join(map(lambda item: u'{}:{}'.format(*item), self.toStatus().items()))
        self.log.warning(t)

        # 一键撤单
        self.cancelAll()

        # 下平仓单
        if self.pos > 0:
            # 平多
            price = self.bm.lastTick.lowerLimit if self.bm.lastTick else self.bar.low
            volume = self.pos
            self.sell(price, volume)
        elif self.pos < 0:
            # 平空
            price = self.bm.lastTick.upperLimit if self.bm.lastTick else self.bar.high
            volume = abs(self.pos)
            self.cover(price, volume)

        if not self.isBackTesting():
            self.log.warning(u'一键平仓')

        self.isCloseoutVaild = False

    def toStatus(self):
        dic = {
            'pos': self.pos,
            'rtBalance': self.rtBalance,
            'averagePrice': self.averagePrice,
        }
        return dic

    def positionErrReport(self, err):
        self.log.error(err)

    @property
    def maxHands(self):
        return int(self.capital / (self.bar.close * self.size * self.marginRate))

    def orderUntilTradingTime(self):
        """
        使用子线程在等待进入连续交易时再下单
        :return:
        """
        if self.xminBar and self.am and self.inited and self.trading:
            if self.isBackTesting():
                # 回测时
                self._orderOnThreading()
            else:
                if self.bm and self.bm.lastTick and self.bm.lastTick.datetime:
                    _now = self.bm.lastTick.datetime
                else:
                    _now = arrow.now().datetime

                _futures = _now + datetime.timedelta(seconds=2)
                if tt.get_trading_status(self.vtSymbol, _futures) == tt.continuous_auction:
                    # 已经进入连续竞价的阶段，直接下单
                    self.log.info(u'已经处于连续竞价阶段')
                    waistSeconds = 0
                else:  # 还没进入连续竞价，使用一个定时器
                    self.log.info(u'尚未开始连续竞价')
                    moment = waitToContinue(self.vtSymbol, _futures)
                    wait = moment - _now
                    # 提前2秒下停止单
                    waistSeconds = wait.total_seconds()
                    self.log.info(u'now:{} {}后进入连续交易, 需要等待 {}'.format(arrow.now().datetime, moment, wait))

                # 至少要等待5秒以上，等待其他策略的 onStart 完成
                waistSeconds = max(5, waistSeconds)
                Timer(waistSeconds, self._orderOnThreading).start()
        else:
            self.log.warning(
                u'无法确认条件单的时机 {} {} {} {}'.format(not self.xminBar, not self.am, not self.inited, not self.trading))

    def _orderOnThreading(self):
        """
        在 orderOnTradingTime 中调用该函数，在子线程中下单
        :return:
        """
        raise NotImplementedError(u'尚未定义')

    def isOrderInContinueCaution(self):
        """
        是否处于可下单的连续竞价中
        :return:
        """
        if self.bm and self.bm.lastTick and self.bm.lastTick.datetime:
            _now = self.bm.lastTick.datetime
            # 当前和未来2秒都要处于连续竞价阶段
            if tt.get_trading_status(self.vtSymbol, _now) == tt.continuous_auction:
                _futures = _now + datetime.timedelta(seconds=2)
                if tt.get_trading_status(self.vtSymbol, _futures) == tt.continuous_auction:
                    return True
        return False

    def _calHandsByLoseCountPct(self, hands, flinch):
        """
        随着连败按照比例加仓
        :param flinch:
        :return:
        """
        if flinch == 0:
            return hands

        # 按照连败计数来使用仓位，每多败1次，就多1点仓位，最大不超过1
        pct = min(1, self.loseCount * 1. / flinch)
        # 最少要有1手仓位
        return max(1, int(hands * pct))

    def _calHandsByLoseCount(self, hands, flinch):
        """
        保持轻仓，连败 flinch 次之后满仓
        :param hands:
        :param flinch:
        :return:
        """
        if self.loseCount < flinch:
            hands = min(1, hands)

        return hands

    def _calHandsByWinCountPct(self, hands, flinch):
        """
        随着连胜按照比例加仓
        :param flinch:
        :return:
        """
        if flinch == 0:
            return hands

        # 按照连胜计数来使用仓位，每多胜1次，就减少1点仓位，最小仓位为1手
        pct = max(0, (flinch - self.winCount) * 1. / flinch)
        # 最少要有1手仓位
        return max(1, int(hands * pct))

    def canProcessStopOrder(self):
        """
        检查停止单是否已经锁定
        :return:
        """
        if self.stopOrdering.isSet():
            self.stopOrderingCount += 1
            if self.stopOrderingCount == 5 * 2:
                self.log.warning(u'策略长时间被停止单锁定 {} 秒'.format(int(self.stopOrderingCount / 2)))
            if self.stopOrderingCount == 30 * 2:
                self.log.warning(u'策略长时间被停止单锁定 {} 秒'.format(int(self.stopOrderingCount / 2)))

            return False
        else:
            return True

    def clearStopOrdering(self):
        self.stopOrdering.clear()
        self.log.info(u'策略内解除停止单锁定')
        self.stopOrderingCount = 0

    def setStopOrdering(self):
        self.log.info(u'策略内停止单锁定')
        self.stopOrdering.set()

    def monitorSplippage(self, trade):
        """
        监控滑点，滑点过大时警示
        :return:
        """
        # 滑点过大警告, 滑点超过 - overSplipage 算作滑点过大
        overSplipage = -self._setting.get('overSplipage', self.overSplipage)
        if trade.splippage and trade.splippage / self.priceTick <= overSplipage:
            self.log.warning(
                u'成交滑点过大,方向 {} 触发价  {} 成交价 {} 滑点 {} / {} <= {}'.format(trade.direction, trade.stopPrice, trade.price,
                                                                      trade.splippage, self.priceTick, overSplipage))

    def _onTrade(self, trade):
        """
        onTrade 的常规逻辑
        :param trade:
        :return:
        """
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

        return originCapital, charge, profile

    def printOutOnTrade(self, trade, OFFSET_CLOSE_LIST, originCapital, charge, profile):
        if trade.offset in OFFSET_CLOSE_LIST:
            textList = [u'{} {} {} {}'.format(self.tradingDay, trade.price, trade.direction, trade.offset)]
            textList.append(u'资金变化 {} -> {}'.format(originCapital, self.capital))
            textList.append(u'仓位{} -> {}'.format(self.prePos, self.pos))
            textList.append(u'手续费 {} 利润 {}'.format(round(charge, 2), round(profile, 2)))
            textList.append(u'**********************')
            print(u'\n'.join(textList))

    def loadBarOnInit(self):
        """
        常规加载
        :return:
        """
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

########################################################################
class TargetPosTemplate(CtaTemplate, vtTargetPosTemplate):
    def onBar(self, bar1min):
        vtTargetPosTemplate.onBar(self, bar1min)
        CtaTemplate.onBar(self, bar1min)


#########################################################################
class BarManager(VtBarManager):
    def __init__(self, strategy, onBar, xmin=0, onXminBar=None):
        super(BarManager, self).__init__(onBar, xmin, onXminBar)
        self.strategy = strategy
        self.preBar = None  # 前一个1分钟K线对象
        self.preXminBar = None  # 前一个X分钟K线对象
        self.hourBar = None # 当前1小时K线
        self.preHourBar = None # 前一个1小时K线

        # 当前已经加载了几个1min bar。当前未完成的 1minBar 不计入内
        self.count = 0

    @property
    def log(self):
        return self.strategy.log

    @property
    def trading(self):
        return self.strategy.trading

    @property
    def inited(self):
        return self.strategy.inited

    # ----------------------------------------------------------------------
    def updateTick(self, tick):
        """
        onTick -> updateTick -> updateBar -> onBar -> updateXminBar -> onXminBar
        :param tick:
        :return:
        """
        if self.lastTick is None and not self.strategy.isBackTesting():
            # 第一个 tick 就比当前时间偏离，则
            if abs((tick.datetime - arrow.now().datetime).total_seconds()) > 60 * 10:
                return

        # 剔除错误数据
        if self.lastTick and tick.datetime - self.lastTick.datetime > datetime.timedelta(seconds=60 * 20):
            # 如果当前 tick 比上一个 tick 差距达到 20分钟没成交的合约，则认为是错误数据
            # 20分钟是早盘10:15 ~ 10:30 的休市时间
            # CTA 策略默认使用比较活跃的合约
            # 中午休市的时候必须重启服务，否则的话 lastTick 和 新tick之间的跨度会过大
            self.log.warning(u'剔除错误数据 {} {}'.format(self.lastTick.datetime, tick.datetime))
            return

        # 更新 bar
        self.updateBar(tick)

        # 缓存Tick
        self.lastTick = tick

    def updateBar(self, tick):
        """
        onTick -> updateTick -> updateBar -> onBar -> updateXminBar -> onXminBar
        :param tick:
        :return:
        """
        # 尚未创建对象
        newMinute = False  # 默认不是新的一分钟

        if not self.bar:
            self.bar = VtBarData()
            newMinute = True
        elif self.bar.datetime.minute != tick.datetime.minute:
            # 新的一分钟
            # 生成上一分钟K线的时间戳
            # 上一根k线的时间戳为，当前分钟的 0秒
            dt = self.bar.datetime.replace(second=0, microsecond=0)
            dt += datetime.timedelta(minutes=1)
            self.bar.datetime = dt
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            # 先推送当前的bar，再从 tick 中更新数据
            self.onBar(self.bar)

            # 创建新的K线对象
            self.preBar, self.bar = self.bar, VtBarData()
            newMinute = True

        # 初始化新一分钟的K线数据
        if newMinute:
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
        # 累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, tick.lastPrice)
            self.bar.low = min(self.bar.low, tick.lastPrice)

        # 通用更新部分
        self.bar.close = tick.lastPrice
        self.bar.datetime = tick.datetime
        self.bar.openInterest = tick.openInterest

        if self.lastTick:
            self.bar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量

    def updateHourBar(self, bar):
        """
        生成1小时线
        :param bar:
        :return:
        """
        if self.strategy.isBackTesting():
            self.bar = bar

        self.count += 1

        newhourBar = self.hourBar is None

        # 尚未创建对象
        if newhourBar:
            self.preHourBar, self.hourBar = self.hourBar, VtBarData()

            self.hourBar.vtSymbol = bar.vtSymbol
            self.hourBar.symbol = bar.symbol
            self.hourBar.exchange = bar.exchange

            self.hourBar.open = bar.open
            self.hourBar.high = bar.high
            self.hourBar.low = bar.low
            # 累加老K线
        else:
            self.hourBar.high = max(self.hourBar.high, bar.high)
            self.hourBar.low = min(self.hourBar.low, bar.low)

        # 通用部分
        self.hourBar.close = bar.close
        self.hourBar.datetime = bar.datetime
        self.hourBar.openInterest = bar.openInterest
        self.hourBar.volume += int(bar.volume)

        # X分钟已经走完
        if self.count % self.xmin == 0:  # 可以用X整除
            # 结束的 bar 的时间戳，就是 hourBar 的时间戳
            self.hourBar.datetime = bar.datetime
            self.hourBar.date = self.hourBar.datetime.strftime('%Y%m%d')
            self.hourBar.time = self.hourBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onhourBar(self.hourBar)



    def updateXminBar(self, bar):
        """
        onTick -> updateTick -> updateBar -> onBar -> updateXminBar -> onXminBar
        :param bar:
        :return:
        """
        if self.strategy.isBackTesting():
            self.bar = bar

        self.count += 1
        newXminBar = False
        if self.count % self.xmin == 1:
            # 新的K先后的第一个1分钟
            newXminBar = True
            # 清空老K线缓存对象

        # 尚未创建对象
        if newXminBar or self.xmin == 1:
            self.preXminBar, self.xminBar = self.xminBar, VtBarData()

            self.xminBar.vtSymbol = bar.vtSymbol
            self.xminBar.symbol = bar.symbol
            self.xminBar.exchange = bar.exchange

            self.xminBar.open = bar.open
            self.xminBar.high = bar.high
            self.xminBar.low = bar.low
            # 累加老K线
        else:
            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)

        # 通用部分
        self.xminBar.close = bar.close
        self.xminBar.datetime = bar.datetime
        self.xminBar.openInterest = bar.openInterest
        self.xminBar.volume += int(bar.volume)

        # X分钟已经走完
        if self.count % self.xmin == 0:  # 可以用X整除
            # 结束的 bar 的时间戳，就是 xminBar 的时间戳
            self.xminBar.datetime = bar.datetime
            self.xminBar.date = self.xminBar.datetime.strftime('%Y%m%d')
            self.xminBar.time = self.xminBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onXminBar(self.xminBar)


#########################################################################
class ArrayManager(VtArrayManager):
    def __init__(self, size=100):
        size += 1
        super(ArrayManager, self).__init__(size)

    # ----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)

        if array:
            return result
        return result[-1]

    def toHtml(self):
        """

        :return:
        """
        od = OrderedDict([
            ('open', self.openArray[-5:]),
            ('high', self.highArray[-5:]),
            ('low', self.lowArray[-5:]),
            ('close', self.closeArray[-5:]),
            ('volume', self.volumeArray[-5:]),
        ])

        return pd.DataFrame(od).to_html()

    # ----------------------------------------------------------------------
    def ma(self, n, array=False):
        """简单均线"""
        result = talib.MA(self.close, n)
        if array:
            return result
        return result[-1]

    def tr(self, array=False):
        """TR指标"""
        result = talib.TRANGE(self.high, self.low, self.close)

        if array:
            return result
        return result[-1]
