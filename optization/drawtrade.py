# coding=utf-8
import arrow
from myplot.kline import *
import myplot.kline as mk
import json
from mystring import MyConfigParser

logging.basicConfig(level=logging.INFO)


class DrawTrade(object):
    """
    绘制模拟盘/实盘的成交图
    """
    def __init__(self, sql, configPath='drawtrade.ini', startTradingDay=None, endTradingDay=None):
        self.sql = sql
        self.config = MyConfigParser()
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        # K线的选取范围，也决定了成交图的范围
        self.underlyingSymbol = self.config.autoget('DrawTrade', 'underlyingSymbol')
        self.dropTradeIDsFile = self.config.autoget('DrawTrade', 'dropTradeIDsFile')
        self.drawFile = self.config.autoget('DrawTrade', 'drawfile')
        self.startTradingDay = startTradingDay
        self.endTradingDay = endTradingDay

        self.bars = None # K 线数据
        self.originTrl = None # 成交单

    def loadBar(self):

        kwarg = dict(self.config.autoitems('ctp_mongo'))

        self.bars = mk.qryBarsMongoDB(
            self.underlyingSymbol,
            startTradingDay=self.startTradingDay,
            endTradingDay=self.endTradingDay,
            **kwarg
        )


    def loadTrade(self):
        """

        :return:
        """
        # 加载原始成交单
        kwarg = dict(self.config.autoitems('cta_mongo'))

        self.matcher = qryTradeListMongodb(
            sql=self.sql,
            **kwarg
        )

    def filterTrade(self):
        """
        过滤掉成交单中的东西
        :return:
        """
        # col = ['tradeID', 'datetime', 'offset', 'direction', 'price', 'volume', 'pos']

        # 如果是指定日期
        df = self.matcher.df.copy()
        if self.startTradingDay:
            df = df[df.datetime >= self.startTradingDay]
            if df.iloc[0].offset != u'开仓':
                df = self.matcher.df = df.iloc[1:]
        if self.endTradingDay:
            df = df[df.datetime <= self.endTradingDay]
        # 剔除指定的 TradeID
        # with open('/Users/lamter/workspace/SlaveO/svnpy/optization/droptradeid.json', 'r') as f:
        with open(self.dropTradeIDsFile, 'r') as f:
            lis = json.load(f)
            for d in lis:
                if d['name'] == self.sql['name'] and d['symbol'] == self.sql['symbol']:
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
        df = df
        # 重新生成实例 DealMatcher 计算，不能直接替换 matcher.df 计算
        matcher = DealMatcher(df)
        matcher.do()
        self.originTrl = matcher.originTrl

    def draw(self):
        """
        重新绘制成交图
        :return:
        """

        tradeOnKlinePlot = tradeOnKLine('1T', self.bars, self.originTrl, width=3000, height=1350)

        tradeOnKlinePlot.render(self.drawFile)
        # tradeOnKlinePlot.render(u'/Users/lamter/Downloads/模拟盘成交图.html')


if __name__ == '__main__':
    dt = DrawTrade({})
    dt.loadBar()
    dt.loadTrade()
    dt.filterTrade()
