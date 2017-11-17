# coding:utf-8
from vnpy.trader.app.ctaStrategy.svtCtaBacktesting import BacktestingEngine
from vnpy.trader.app.ctaStrategy.strategy import STRATEGY_CLASS

globals().update(STRATEGY_CLASS)


def runBacktesting(vtSymbol, setting, strategyClass, mode=BacktestingEngine.BAR_MODE, isShowFig=True,
                   isOutputResult=True):
    """

    :param vtSymbol: 合约编号
    :param dbn: 使用的数据库
    :param slippage: 滑点
    :param rate: 手续费
    :param size: 每手的单位数量
    :param priceTick: 价格最小变动
    :param capital: 回测用的资金
    :param setting: 策略参数
    :param startDate: 回测开始日期
    :param endDate: 回测结束日期
    :param mode: 使用的数据模式 BAR_MODE or TICK_MODE
    :return:1
    """
    # 创建回测引擎对象
    engine = BacktestingEngine()

    # 设置回测使用的数据
    engine.setBacktestingMode(mode)  # 设置引擎的回测模式为K线
    engine.setSymbol(vtSymbol)  # 设置该次回测使用的合约
    engine.setShowFig(isShowFig)
    engine.setOutputResult(isOutputResult)

    # 在引擎中创建策略对象
    # 策略参数配置
    setting['vtSymbol'] = vtSymbol
    engine.initStrategy(globals()[strategyClass], setting)  # 创建策略对象
    return engine


if __name__ == '__main__':
    vtSymbol = 'rb1805'

    setting = {
        'capital': 100000,
        'vtSymbol': vtSymbol,
    }
    setting.update(
        {"atrWindow":30,"barXmin":15,"bollDev":2.4,"bollWindow":18,"cciWindow":10,"slMultiplier":5.2}
    )

    engine = runBacktesting(
        vtSymbol=vtSymbol,
        setting=setting,
        strategyClass='SvtBollChannelStrategy',
        mode=BacktestingEngine.BAR_MODE,
        isShowFig=False,
        isOutputResult=True,
    )

    # 运行回测
    engine.runBacktesting()  # 运行回测
    # 输出回测结果
    engine.showDailyResult()
    engine.showBacktestingResult()
