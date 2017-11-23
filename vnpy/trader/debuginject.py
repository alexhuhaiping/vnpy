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
    s.cover(s.bm.bar.close, 1, True)
    s.log.debug(u'下单完成 {}'.format(s.bm.bar.close))

def short():
    s = getStrategy(vtSymbol)
    # s.pos = -15
    s.short(s.bm.bar.close, 1, True)
    s.log.debug(u'下单完成 {}'.format(s.bm.bar.close))

def sell():
    s = getStrategy(vtSymbol)
    # s.pos = -15
    s.sell(s.bm.bar.close, 1, True)
    s.log.debug(u'下单完成 {}'.format(s.bm.bar.close))


def showStopOrder():
    # s = getStrategy(vtSymbol)
    for os in ce.stopOrderDict.values():
        print(os.toHtml())

def showBar():
    s = getStrategy(vtSymbol)
    s.log.info(u'{}'.format(s.bar.datetime))

def showLastTick():
    s = getStrategy(vtSymbol)
    s.log.info(u'{}'.format(s.bm.lastTick.datetime))



vtSymbol = 'hc1805'

def run():
    return
    load()
    me.log.debug('====================================================')

    showBar()
    # showStopOrder()
    # sell()
    # short()
    # cover()
    # checkHands(me)

    me.log.debug('====================================================')