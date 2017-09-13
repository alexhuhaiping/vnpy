# coding:utf-8
import vnpy.trader.app.ctaStrategy.svtCtaBacktesting
import vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel
import vnpy.trader.app.ctaStrategy.svtCtaTemplate

from vnpy.trader.app.ctaStrategy.svtCtaBacktesting import BacktestingEngine
from vnpy.trader.app.ctaStrategy.ctaBacktesting import OptimizationSetting, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategyDonchianChannel import DonchianChannelStrategy

# 创建回测引擎对象
engine = BacktestingEngine()

# 设置回测使用的数据
vtSymbol = 'rb1801'
engine.setBacktestingMode(engine.BAR_MODE)    # 设置引擎的回测模式为K线
engine.setDatabase(MINUTE_DB_NAME, vtSymbol)  # 设置使用的历史数据库
engine.setStartDate('2017-02-01 00:00:00+08:00')               # 设置回测用的数据起始日期
# engine.setEndDate('2017-02-25 00:00:00+08:00')               # 设置回测用的数据起始日期

# 配置回测引擎参数
engine.setSlippage(1)     # 设置滑点为股指1跳
engine.setRate(0.2/10000)   # 设置手续费万0.3
engine.setSize(10)         # 设置股指合约大小
engine.setPriceTick(1)    # 设置股指最小价格变动
engine.setCapital(100000)  # 设置回测本金

# 在引擎中创建策略对象
d = {
    'unitsNum': 4,
    'hands': 0,
    'vtSymbol': vtSymbol,
    'barPeriod': 15, # bar 周期
    'atrPeriod': 26,
}
# 策略参数配置
engine.initStrategy(DonchianChannelStrategy, d)    # 创建策略对象

# 运行回测515
engine.runBacktesting()          # 运行回测