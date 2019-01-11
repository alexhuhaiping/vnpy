# coding:utf-8

from itertools import chain
import arrow

from myplot.kline import *
from drawbacktestingtrade import DrawBacktestingTrade
from drawtrade import DrawTrade

################################
# 运行回测，生成成交图
dbt = DrawBacktestingTrade(configPath='drawbttrade.ini',startTradingDay=arrow.get('2019-01-09 00:00:00+08').datetime)
dbt.clearCollection()  # 清空数据库
dbt.runArg()  # 生成参数
dbt.runBacktesting()# 批量回测

# # 加载数据并绘制成交图
dbt.loadBar()
dbt.loadTrade()
dbt.draw()
################################




################################
# 模拟盘成交
sql = {
    'symbol': 'AP905',
    'class': 'ContrarianAtrStrategy',
    'name': u'苹果_定点ATR反转20min回测对比',
}
dt = DrawTrade(sql, configPath='drawtrade.ini', startTradingDay=arrow.get('2019-01-09 00:00:00+08').datetime)
dt.loadBar()
dt.loadTrade()
dt.filterTrade()
dt.draw()
################################

originTrl = chain(dt.originTrl, dbt.originTrl)
# # 合并绘制成交图
tradeOnKlinePlot = tradeOnKLine('1T', dt.bars, originTrl, width=3000, height=1350)
tradeOnKlinePlot.render(u'/Users/lamter/Downloads/叠加成交图.html')


