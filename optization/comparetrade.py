# coding:utf-8

from itertools import chain
import arrow

from myplot.kline import *
from drawbacktestingtrade import DrawBacktestingTrade
from drawtrade import DrawTrade

################################
# 运行回测，生成成交图
# startTradingDay=arrow.get('2019-01-14 00:00:00+08').datetime
startTradingDay=arrow.get('2018-12-01 00:00:00+08').datetime
dbt = DrawBacktestingTrade('drawtrade_backtesting.ini', startTradingDay)
dbt.clearCollection()  # 清空数据库
dbt.runArg()  # 生成参数
dbt.runBacktesting()# 批量回测

# dbt.loadTrade()

# 加载数据并绘制成交图
# dbt.loadBar()
# dbt.draw('1T')
################################



# ################################
# # 实盘盘成交
# # startTradingDay=arrow.get('2019-01-10 00:00:00+08').datetime
# drm = DrawTrade('drawtrade_realmoney.ini', )
# drm.loadTrade()
# drm.filterTrade()
# drm.loadBar()
# drm.draw()
# ################################

# ################################
# # 模拟盘成交
# # startTradingDay = drm.matcher.startTradingDay # 取实盘的第一笔成交开始做对比
# dsim = DrawTrade('drawtrade_sim.ini', )
# dsim.loadTrade()
# dsim.filterTrade()
# dsim.loadBar()
# dsim.draw()
# ################################

# originTrl = list(chain(
#     dbt.originTrl,
#     dsim.originTrl,
#     # drm.originTrl
#
# ))
# # # 合并绘制成交图
# tradeOnKlinePlot = tradeOnKLine('1T', dsim.bars, originTrl, width=3000, height=1350)
# tradeOnKlinePlot.render(u'/Users/lamter/Downloads/叠加成交图.html')


