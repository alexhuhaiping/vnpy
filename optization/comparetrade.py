# coding:utf-8

import logging
logging.basicConfig(level=logging.INFO)
from itertools import chain
import arrow

from myplot.kline import *
from drawbacktestingtrade import DrawBacktestingTrade
from drawtrade import DrawTrade

PERIOD = '1T'
originTrlList = []

################################
# 实盘盘成交
# startTradingDay=arrow.get('2019-01-10 00:00:00+08').datetime
drm = DrawTrade('drawtrade_realmoney.ini', )
originTrlList.append(drm)
drm.loadTrade()
drm.filterTrade()
drm.loadBar()
drm.draw(PERIOD, 2000, 1000)
################################

###############################
# 运行回测，生成成交图
try:
    startTradingDay = drm.matcher.startTradingDay # 取实盘的第一笔成交开始做对比
except AttributeError:
    startTradingDay = arrow.get('2019-01-24 00:00:00+08').datetime
    # startTradingDay = None
dbt = DrawBacktestingTrade('drawtrade_backtesting.ini',startTradingDay=startTradingDay)
originTrlList.append(dbt)

dbt.clearCollection()  # 清空数据库
dbt.runArg()  # 生成参数
dbt.runBacktesting()# 批量回测

# 加载成交单
dbt.loadTrade()

# 加载数据并绘制成交图
dbt.loadBar()

# 连续绘制不同 barXmin 的成交单
# for i in [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120]:
#     optsv = 'AP,"barXmin":{}'.format(i)
#     dbt.config.set('DrawBacktestingTrade', 'optsv', optsv)
#     dbt.draw()
dbt.draw(PERIOD, 2000, 1000)
################################

# ################################
# # 模拟盘成交
# startTradingDay = drm.matcher.startTradingDay # 取实盘的第一笔成交开始做对比
# dsim = DrawTrade('drawtrade_sim.ini', )
# originTrlList.append(dbt)
# dsim.loadTrade()
# dsim.filterTrade()
# dsim.loadBar()
# dsim.draw(PERIOD, 2000, 1000)
# ################################

originTrl = list(chain(
    *[d.originTrl for d in originTrlList]

))
# # 合并绘制成交图
tradeOnKlinePlot = tradeOnKLine(
    PERIOD, dbt.bars, originTrl, width=2000, height=1000
)
tradeOnKlinePlot.render(u'/Users/lamter/Downloads/叠加成交图.html')
