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
    return [s for s in list(ce.strategyDict.values()) if s.vtSymbol == symbol and isinstance(s, CtaTemplate)]


def checkHands():
    s = getStrategy(vtSymbol)
    # me.log.info('{}'.format(s.hands))


def showLastTick():
    s = getStrategy(vtSymbol)
    s.log.info('{}'.format(s.bm.lastTick.datetime))


def showBar():
    s = getStrategy(vtSymbol)

    s.log.info('{}'.format(str(s.am.highArray)))


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
    s.log.info('生成订单')
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
    s.log.debug('{}'.format(tradingDay))

    trade.datetime = dt
    trade.tradingDay = tradingDay

    s.saveTrade(trade)


def testToStatus():
    s = getStrategy(vtSymbol)
    self = s
    t = '\n'.join(['{}:{}'.format(*item) for item in list(self.toStatus().items())])
    s.log.debug(t)


def toHtml():
    s = getStrategy(vtSymbol)
    s.log.info(str(s.toHtml()))


def closeout():
    s = getStrategy(vtSymbol)
    s.log.info('强制平仓')
    s.closeout()
    s.log.info('强制平仓下单完成')


def checkPosition():
    s = getStrategy(vtSymbol)
    # s._pos += 1
    s.log.debug(s.trading)


def checkPositionDetail():
    for k, detail in list(me.dataEngine.detailDict.items()):
        print(k)
        print((detail.output()))


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

    s.log.debug('下单完成 {}'.format(price))


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
    print((me.dataEngine.orderDict))


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
    print((list(ce.workingStopOrderDict.keys())))


def buy():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        s.log.debug('测试 buy')
        # s.pos = -15

        price = s.bm.bar.close - 2
        volume = 1
        stop = True

        s.buy(price, volume, stop)

        s.log.debug('下单完成 {}'.format(price))


def cover():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        price = s.bm.bar.close - 1
        volume = 2
        stop = False

        s.cover(price, volume, stop)

        s.log.debug('下单完成 {}'.format(s.bm.bar.close))
        break


def sell():
    sList = getStrategy(vtSymbol)
    # print(sList)
    for s in sList:
        # s.log.info(u'{}'.format(vtSymbol))
        # s.pos = -15

        price = s.bm.bar.close
        volume = 2
        stop = True
        s.sell(price, volume, stop)
        s.log.debug('下单完成 {}'.format(price))


def orderToShow():
    sList = getStrategy(vtSymbol)
    print(sList)
    for s in sList:

        for vtOrderID, order in list(s.orders.items()):
            print('+++++++++++')
            for k, v in list(order.__dict__.items()):
                print(('{} {}'.format(k, v)))

        orderList = s.ctaEngine.getAllOrderToShow(s.name)
        # print(len(orderList), len(s.orders))
        for order in orderList:
            print('+++++++++++')
            for k, v in list(order.items()):
                print(('{} {}'.format(k, v)))

            showStopOrder


def showStopOrder():
    sList = getStrategy(vtSymbol)
    stopOrderIDs = ce.getAllStopOrdersSorted(vtSymbol)
    stopOrderIDs.sort(key=lambda s: (s.direction, s.stopProfile, s.price))
    me.log.info('停止单数量{}'.format(len(ce.stopOrderDict)))
    price = 3749.0
    for os in stopOrderIDs:
        if os.direction == '空' and os.status == '等待中':
            # if os.direction == u'空' and os.status == u'等待中':
            if os.status == '等待中':
                pass
                os.price = price
                price -= 2
            log = ''
            for k, v in list(os.toHtml().items()):
                log += '{}:{} '.format(k, v)
            me.log.info(log)


def checkMargin():
    sList = getStrategy(vtSymbol)
    s = sList[0]
    print((s.marginRate))

def reSubscribe():
    sList = getStrategy(vtSymbol)
    for s in sList:
        if s.name == '焦炭_经典海龟120min':
            break
    print((s.name))

# vtSymbol = 'AP905'
vtSymbol = 'j1905'
import logging


def run():
    load()
    return
    me.log.info('====================================================')
    reSubscribe()

    # checkMargin()
    # showStopOrder()

    # cover()
    # sell()
    # buy()

    # showWorkingStopOrderDic()
    # saveStrategy()


    # sendStopProfileOrder()

    # cancelOrder()
    # checkDataEngineOrder()
    # sendOrder()
    # short()


    # orderToShow()


    # strategyOrder()

    # checkContinueCaution()

    # checkPositionDetail()
    # showStopOrder()

    # checkPosition()

    # toHtml()
    # testToStatus()
    # saveTradeData()
    # testSaveTrade()
    # showLastTick()
    # showBar()
    # closeout()
    # checkHands(me)

    me.log.debug('----------------------------------------------------')
