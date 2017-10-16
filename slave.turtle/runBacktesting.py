# coding:utf-8
import vnpy.trader.app.ctaStrategy.svtCtaBacktesting
import vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel
import vnpy.trader.app.ctaStrategy.svtCtaTemplate

from vnpy.trader.app.ctaStrategy.svtCtaBacktesting import BacktestingEngine
from vnpy.trader.app.ctaStrategy.ctaBacktesting import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel import DonchianChannelStrategy


def runBacktesting(vtSymbol, dbn, slippage, rate, size, priceTick, capital, marginRate, setting, startDate, endDate=None,
                   mode=BacktestingEngine.BAR_MODE):
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
    :return:
    """
    # 创建回测引擎对象
    engine = BacktestingEngine()

    # 设置回测使用的数据
    engine.setSymbol(vtSymbol) # 设置该次回测使用的合约

    # engine.setBacktestingMode(mode)  # 设置引擎的回测模式为K线
    # engine.setDatabase(dbn, vtSymbol)  # 设置使用的历史数据库
    # engine.setStartDate(startDate)  # 设置回测用的数据起始日期
    # if endDate:
    #     engine.setEndDate(endDate)  # 设置回测用的数据起始日期

    # # 配置回测引擎参数
    # engine.setSlippage(slippage)  # 设置滑点为股指1跳
    # engine.setRate(rate)  # 设置手续费万0.3
    # engine.setSize(size)  # 设置股指合约大小
    # engine.setPriceTick(priceTick)  # 设置股指最小价格变动
    # engine.setCapital(capital)  # 设置回测本金
    # engine.setMarginRate(marginRate)  # 设置保证金比例

    # 在引擎中创建策略对象
    setting
    # 策略参数配置
    engine.initStrategy(DonchianChannelStrategy, setting)  # 创建策略对象

    # 运行回测
    engine.runBacktesting()  # 运行回测

    # 输出回测结果
    engine.showDailyResult()


if __name__ == '__main__':
    vtSymbol = 'hc1801'

    runBacktesting(
        vtSymbol=vtSymbol,
        slippage=1,
        rate=0.2 / 10000,
        size=10,
        priceTick=1,
        capital=100000,
        dbn=MINUTE_DB_NAME,
        marginRate=0.15,
        setting={
            'unitsNum': 4,
            'vtSymbol': vtSymbol,
            'barPeriod': 9,  # bar 周期
            'atrPeriod': 14,
            'maxCD': 1,
            'sys2Vaild': True,
        },
        startDate='2017-04-01 00:00:00+08:00',
        mode=BacktestingEngine.BAR_MODE,
    )
