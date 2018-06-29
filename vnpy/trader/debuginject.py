# encoding: UTF-8

me = None
ce = None


def load():
    global me
    from vnpy.trader.svtEngine import MainEngine
    assert isinstance(me, MainEngine)
    from vnpy.trader.app.ctaStrategy import appName
    global ce
    ce = me.getApp(appName)
    from vnpy.trader.app.ctaStrategy.svtCtaEngine import CtaEngine
    assert isinstance(ce, CtaEngine)


def getStrategy(symbol):
    global me

    from vnpy.trader.app.ctaStrategy.svtCtaTemplate import CtaTemplate
    for s in ce.strategyDict.values():
        if s.vtSymbol == symbol:
            isinstance(s, CtaTemplate)
            return s


def checkHands():
    s = getStrategy(vtSymbol)
    # me.log.info('{}'.format(s.hands))


def cover():
    s = getStrategy(vtSymbol)
    # s.pos = -15

    price = s.bm.bar.close - 1
    volume = 20
    stop = False

    s.cover(price, volume, stop)
    s.cover(price, volume, stop)
    s.cover(price, volume, stop)

    s.log.debug(u'下单完成 {}'.format(s.bm.bar.close))


def short():
    s = getStrategy(vtSymbol)
    # s.pos = -15

    price = int(s.bm.bar.close - 1)
    volume = 1
    stop = False

    s.short(price, volume, stop)

    s.log.debug(u'下单完成 {}'.format(price))


def showLastTick():
    s = getStrategy(vtSymbol)
    s.log.info(u'{}'.format(s.bm.lastTick.datetime))


def showBar():
    s = getStrategy(vtSymbol)

    s.log.info(u'{}'.format(str(s.am.highArray)))


def testSaveTrade():
    # 测试保存成交
    from vnpy.trader.vtObject import VtTradeData
    from vnpy.trader.vtConstant import (EMPTY_STRING, EMPTY_UNICODE,
                                        EMPTY_FLOAT, EMPTY_INT)

    import arrow
    import tradingtime as tt
    vtTrade = VtTradeData()
    vtTrade.symbol = vtSymbol  # 合约代码
    vtTrade.exchange = 'SHEF'  # 交易所代码
    vtTrade.vtSymbol = vtSymbol  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

    vtTrade.tradeID = EMPTY_STRING  # 成交编号
    vtTrade.vtTradeID = EMPTY_STRING  # 成交在vt系统中的唯一编号，通常是 Gateway名.成交编号

    vtTrade.orderID = EMPTY_STRING  # 订单编号
    vtTrade.vtOrderID = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

    # 成交相关
    vtTrade.direction = EMPTY_UNICODE  # 成交方向
    vtTrade.offset = EMPTY_UNICODE  # 成交开平仓
    vtTrade.price = EMPTY_FLOAT  # 成交价格
    vtTrade.volume = EMPTY_INT  # 成交数量
    vtTrade.tradeTime = arrow.now().datetime  # 成交时间
    isTradingDya, tradingDay = tt.get_tradingday(vtTrade.tradeTime)
    vtTrade.tradingDay = tradingDay  # 交易日

    s = getStrategy(vtSymbol)
    s.log.info(u'生成订单')
    s.saveTrade(vtTrade)


def saveTradeData():
    import arrow
    from datetime import datetime, date, time, timedelta
    from vnpy.trader.vtFunction import LOCAL_TIMEZONE
    import tradingtime as tt

    s = getStrategy(vtSymbol)
    trade = s.trade

    now = arrow.now()
    t = time(*list(map(int, trade.tradeTime.split(':'))))
    dt = datetime.combine(date.today(), t)
    dt = LOCAL_TIMEZONE.localize(dt)
    if now.datetime - dt < timedelta(hours=1):
        # 回报和本地时间差在1个小时内则没有跨日
        pass
    else:
        # 跨日了
        dt = datetime.combine(date.today() - timedelta(days=1), t)
        dt = LOCAL_TIMEZONE.localize(dt)

    _, tradingDay = tt.get_tradingday(dt)
    s.log.debug(u'{}'.format(tradingDay))

    trade.datetime = dt
    trade.tradingDay = tradingDay

    s.saveTrade(trade)


def testToStatus():
    s = getStrategy(vtSymbol)
    self = s
    t = u'\n'.join(map(lambda item: u'{}:{}'.format(*item), self.toStatus().items()))
    s.log.debug(t)


def toHtml():
    s = getStrategy(vtSymbol)
    s.log.info(str(s.toHtml()))


def closeout():
    s = getStrategy(vtSymbol)
    s.log.info(u'强制平仓')
    s.closeout()
    s.log.info(u'强制平仓下单完成')


def checkPosition():
    s = getStrategy(vtSymbol)
    # s._pos += 1
    s.log.debug(s.trading)


def cancelOrder():
    s = getStrategy(vtSymbol)
    s.cancelAll()


def buy():
    s = getStrategy(vtSymbol)
    s.log.debug(u'测试 buy')
    # s.pos = -15

    price = s.bm.bar.close - 0
    volume = 1
    stop = False

    s.buy(price, volume, stop)

    s.log.debug(u'下单完成 {}'.format(price))


def sell():
    s = getStrategy(vtSymbol)

    # s.log.info(u'{}'.format(vtSymbol))
    # s.pos = -15

    price = s.bm.bar.close + 200
    stop = False
    s.sell(price, 1, stop)
    s.log.debug(u'下单完成 {}'.format(price))


def orderToShow():
    print(ce.vtOrderReqToShow)
    for order in ce.vtOrderReqToShow.values():
        log = u''
        for k, v in order.__dict__.items():
            log += u'{}:{} '.format(k, v)

        print(log)


def showStopOrder():
    s = getStrategy(vtSymbol)
    stopOrderIDs = ce.getAllStopOrdersSorted(vtSymbol)
    me.log.info(u'{}'.format(len(ce.stopOrderDict)))
    for os in ce.stopOrderDict.values():
        if os.direction == u'多' and os.status == u'等待中':
            os.price = 116700
            log = u''
            for k, v in os.toHtml().items():
                log += u'{}:{} '.format(k, v)
            me.log.info(log)


def checkPositionDetail():
    for k, detail in me.dataEngine.detailDict.items():
        print(k)
        print(detail.output())



def strategyOrder():
    s = getStrategy(vtSymbol)
    s.orderOnXminBar(s.bm.xminBar)


vtSymbol = 'hc1810'
import logging


def run():
    return
    load()
    me.log.info('====================================================')
    strategyOrder()
    # checkPositionDetail()
    # showStopOrder()
    # orderToShow()

    # buy()
    # cancelOrder()
    # sell()

    # checkPosition()

    # toHtml()
    # testToStatus()
    # saveTradeData()
    # testSaveTrade()
    # showLastTick()
    # showBar()
    # showStopOrder()
    # closeout()
    # sell()
    # short()
    # cover()
    # checkHands(me)

    me.log.debug('====================================================')
