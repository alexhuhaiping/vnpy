# coding:utf-8
import vnpy.trader.app.ctaStrategy.svtCtaBacktesting
import vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel
import vnpy.trader.app.ctaStrategy.svtCtaTemplate

from vnpy.trader.app.ctaStrategy.svtCtaBacktesting import BacktestingEngine
from vnpy.trader.app.ctaStrategy.ctaBacktesting import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel import DonchianChannelStrategy
from vnpy.trader.app.ctaStrategy.strategy.strategySvtBollChannel import BollChannelStrategy


def runBacktesting(vtSymbol, setting, strategyClass, mode=BacktestingEngine.BAR_MODE, isShowFig=True, ):
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

    # 在引擎中创建策略对象
    # 策略参数配置
    setting['vtSymbol'] = vtSymbol
    engine.initStrategy(strategyClass, setting)  # 创建策略对象

    # 运行回测
    engine.runBacktesting()  # 运行回测

    # 输出回测结果
    engine.showDailyResult()
    # engine.showBacktestingResult()
    return engine


if __name__ == '__main__':
    vtSymbol = 'hc1801'

    runBacktesting(
        vtSymbol=vtSymbol,

        # setting={
        #     'unitsNum': 4,
        #     'vtSymbol': vtSymbol,
        #     'barPeriod': 9,  # bar 周期
        #     'atrPeriod': 14,
        #     'maxCD': 1,
        #     'sys2Vaild': True,
        #     'capital': 100000,
        # },
        setting={
            'barXmin': 15,
            'capital': 100000,
            'vtSymbol': vtSymbol,
            # 'bollWindow': 19,  # 布林通道窗口数
            # 'bollDev': 3.4,  # 布林通道的偏差
            # 'cciWindow': 10,  # CCI窗口数
            # 'atrWindow': 30,  # ATR窗口数
            'slMultiplier': 2,  # 计算止损距离的乘数
            # 'risk': 0.02,
        },
        strategyClass=BollChannelStrategy,
        mode=BacktestingEngine.BAR_MODE,
        isShowFig=False,

    )
