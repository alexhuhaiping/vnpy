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
    return [s for s in ce.strategyDict.values() if s.vtSymbol == symbol and isinstance(s, CtaTemplate)]


def checkHands():
    s = getStrategy(vtSymbol)
    # me.log.info('{}'.format(s.hands))



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


def orderToShow():
    print(ce.vtOrderReqToShow)
    for order in ce.vtOrderReqToShow.values():
        log = u''
        for k, v in order.__dict__.items():
            log += u'{}:{} '.format(k, v)

        print(log)


def checkPositionDetail():
    for k, detail in me.dataEngine.detailDict.items():
        print(k)
        print(detail.output())


def checkContinueCaution():
    s = getStrategy(vtSymbol)
    s.ctaEngine.get()


def short():
    s = getStrategy(vtSymbol)
    # s.pos = -15

    price = s.bm.bar.close + s.priceTick * 30
    volume = 1
    stop = False

    s.short(price, volume, stop)

    s.log.debug(u'下单完成 {}'.format(price))


def strategyOrder():
    s = getStrategy(vtSymbol)


def sendOrder():
    s = getStrategy(vtSymbol)
    from vnpy.trader.app.ctaStrategy.ctaBase import CTAORDER_BUY
    orderType = CTAORDER_BUY
    volume = 1
    stop = False
    price = s.bm.bar.close - s.priceTick * 30
    s.sendOrder(orderType, price, volume, stop)


def checkDataEngineOrder():
    print(me.dataEngine.orderDict)


def cancelOrder():
    s = getStrategy(vtSymbol)
    # print(s.ctaEngine.strategyOrderDict)
    s.cancelAll()


def sendStopProfileOrder():
    s = getStrategy(vtSymbol)
    from vnpy.trader.app.ctaStrategy.ctaBase import CTAORDER_BUY
    orderType = CTAORDER_BUY
    volume = 1
    stop = False
    stopProfile = False
    price = s.bm.bar.close + s.priceTick * 30
    s.buy(price, volume, stop)



def saveStrategy():
    s = getStrategy(vtSymbol)
    s.saveDB()


def showWorkingStopOrderDic():
    print(ce.workingStopOrderDict.keys())


def sell():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        # s.log.info(u'{}'.format(vtSymbol))
        # s.pos = -15

        price = s.bm.bar.close
        volume = 3
        stop = True
        s.sell(price, volume, stop)
        s.log.debug(u'下单完成 {}'.format(price))

def showStopOrder():
    sList = getStrategy(vtSymbol)
    stopOrderIDs = ce.getAllStopOrdersSorted(vtSymbol)
    me.log.info(u'停止单数量{}'.format(len(ce.stopOrderDict)))
    for os in ce.stopOrderDict.values():
        if os.direction == u'多' and os.status == u'等待中':
        # if os.direction == u'空' and os.status == u'等待中':
        # if os.status == u'等待中':
            os.price = 104460.0
            log = u''
            for k, v in os.toHtml().items():
                log += u'{}:{} '.format(k, v)
            me.log.info(log)


def buy():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        s.log.debug(u'测试 buy')
        # s.pos = -15

        price = s.bm.bar.close - 0
        volume = 1
        stop = True

        s.buy(price, volume, stop)

        s.log.debug(u'下单完成 {}'.format(price))
def cover():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        price = s.bm.bar.close - 1
        volume = 2
        stop = False

        s.cover(price, volume, stop)

        s.log.debug(u'下单完成 {}'.format(s.bm.bar.close))
        break


vtSymbol = 'AP905'
import logging


def run():
    return
    load()
    me.log.info('====================================================')
    # cover()
    # sell()
    buy()

    # showWorkingStopOrderDic()
    # saveStrategy()

    # showStopOrder()

    # sendStopProfileOrder()

    # cancelOrder()
    # checkDataEngineOrder()
    # sendOrder()
    # short()




    # strategyOrder()

    # checkContinueCaution()

    # checkPositionDetail()
    # showStopOrder()
    # orderToShow()

    # checkPosition()

    # toHtml()
    # testToStatus()
    # saveTradeData()
    # testSaveTrade()
    # showLastTick()
    # showBar()
    # showStopOrder()
    # closeout()
    # checkHands(me)

    me.log.debug('----------------------------------------------------')
