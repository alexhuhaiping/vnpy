# encoding: UTF-8

me = None


def getStrategy(symbol):
    global me
    ce = me.ctaEngine
    from vnpy.trader.app.ctaStrategy.svtCtaEngine import CtaEngine
    assert isinstance(ce, CtaEngine)
    for s in ce.strategyDict.values():
        if s.vtSymbol == symbol:
            return s


def checkHands(me):
    s = getStrategy('hc1801')
    me.log.info('{}'.format(s.hands))


def run():
    return
    global me
    me.log.debug('====================================================')

    checkHands(me)

    me.log.debug('====================================================')
    from vnpy.trader.svtEngine import MainEngine
    assert isinstance(me, MainEngine)
