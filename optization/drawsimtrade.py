# coding=utf-8
from collections import OrderedDict
import ConfigParser
import arrow
from myplot.kline import *
import myplot.kline as mk
import json
logging.basicConfig(level=logging.INFO)

class Config(ConfigParser.SafeConfigParser):
    def optionxform(self, optionstr):
        return optionstr


# 读取设置参数
config = Config()
configPath = 'drawtrade.ini'
with open(configPath, 'r') as f:
    config.readfp(f)

# # 加载K线
endTradingDay = startTradingDay = None
kwarg = dict(config.items('qryBarsMongoDB'))
if 'startTradingDay' in kwarg:
    startTradingDay = kwarg['startTradingDay'] = arrow.get(
        '{} 00:00:00+08:00'.format(kwarg['startTradingDay'])).datetime
if 'endTradingDay' in kwarg:
    endTradingDay = kwarg['endTradingDay'] = arrow.get('{} 00:00:00+08:00'.format(kwarg['endTradingDay'])).datetime
kwarg['port'] = int(kwarg['port'])

documents = mk.qryBarsMongoDB(**kwarg)

# 加载原始成交单
kwarg = dict(config.items('qryTradeListMongodb'))
kwarg['port'] = int(kwarg['port'])
sql = {
    'symbol': 'AP905',
    'class': 'ContrarianAtrStrategy',
    'name': u'苹果_定点ATR反转20min回测对比',
}
matcher = qryTradeListMongodb(
    sql=sql,
    **kwarg
)

col = ['tradeID', 'datetime', 'offset', 'direction', 'price', 'volume', 'pos']

# 剔除异常成交
df = matcher.df.copy()
if startTradingDay:
    df = df[df.datetime >= startTradingDay]
    df = matcher.df = df.iloc[1:]
if endTradingDay:
    df = df[df.datetime <= endTradingDay]
# 剔除指定的 TradeID
with open('/Users/lamter/workspace/SlaveO/svnpy/optization/droptradeid.json', 'r') as f:
    lis = json.load(f)
    for d in lis:
        if d['name'] == sql['name'] and d['symbol'] == sql['symbol']:
            _filter = []
            tradeIDs = d['tradeID'][:]
            for index in df.tradeID.index:
                dfTradeID = df.tradeID.loc[index].strip(' ')
                r = not dfTradeID in tradeIDs
                _filter.append(r)
                if not r:
                    tradeIDs.remove(dfTradeID)
            if _filter:
                logging.info(u'\t{symbol}\t{name}\t剔除成交'.format(**d))
                df = df[pd.Series(_filter, df.index)]
            # 每次只处理一个合约
            break

# 重新生成实例 DealMatcher 计算，不能直接替换 matcher.df 计算
matcher = DealMatcher(df)
matcher.do()

tradeOnKlinePlot = tradeOnKLine('1T', documents, matcher.originTrl, width=3000, height=1350)
tradeOnKlinePlot.render(u'/Users/lamter/Downloads/模拟盘成交图.html')
