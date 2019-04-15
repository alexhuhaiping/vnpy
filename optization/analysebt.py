# coding:utf-8

try:
    import pickle as pickle
except ImportError:
    import pickle
import json
from collections import OrderedDict
import pandas as pd
import pytz
from bson.codec_options import CodecOptions
import pymongo
import configparser

import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


class AnalyseBacktesting(object):
    """
    分析批量回测的数据
    """

    def __init__(self, documents):
        """
        需要给定一个 group 的回测的所有的数据
        :param documents:
        """
        # 原始数据
        self.df = pd.DataFrame(documents)

        # 主力连续的日收益和日净值
        self.dailyReturnRateByOptsv = {}  # {'optsv': pd.DataFrame()}

        # 主力连续的统计汇总
        # self.dailyResultByOptsv = {}  # {'optsv': pd.DataFrame()}
        self.summarizeDialyDF = None  # 汇总各个主力连续在各个参数下的数据

        self.navDf = None

    def init(self):
        """
        初始化数据
        :return:
        """
        self.calNav()
        self.summarizingDaily()

    def calNav(self):
        """
        分组计算净值
        :return:
        """
        # 取出需要计算的数据
        df = self.df.sort_values('最后交易日')
        cols = ['日收益率曲线', 'optsv', 'underlyingSymbol', 'activeEndDate']
        df = df[cols].copy()

        # 根据品种分组
        group = df.groupby('optsv')

        optsvList = list(group.indices.keys())

        # 取得每一组连续的数据
        for optsv in optsvList:
            consisDF = group.get_group(optsv)
            # 排序
            consisDF.sort_values('activeEndDate')
            dailyReturnRateDict = OrderedDict()

            # 取出每日收益率
            consisDF['日收益率曲线'].apply(lambda x: dailyReturnRateDict.update(pickle.loads(x.encode('utf-8'))))

            # 生成日收益和日净值
            self.calDaily(optsv, dailyReturnRateDict)

    def calDaily(self, optsv, dailyReturnRateDict):
        """
        据此计算其他数值汇总参数
        :param optsv:
        :param dailyReturnRateDict: Orderdict()
        :return:
        """

        assert isinstance(dailyReturnRateDict, OrderedDict)

        df = pd.DataFrame({'日收益率': list(dailyReturnRateDict.values())}, index=pd.DatetimeIndex(list(dailyReturnRateDict.keys())))
        df = df.sort_index(inplace=False)

        df['日收益率'] = df['日收益率'].apply(lambda r: -1 if r < -1 else r)

        df['日净值'] = df['日收益率'] + 1
        df['日净值'] = df['日净值']
        df['日净值'] = df['日净值'].cumprod()

        self.dailyReturnRateByOptsv[optsv] = df

        if __debug__:
            self.debugDF = df

    def summarizingDaily(self):
        """
        按日汇总
        :return:
        """
        summarize = []
        for optsv, navDF in list(self.dailyReturnRateByOptsv.items()):
            dic = self._summarizingDaily(optsv, navDF)
            summarize.append(dic)

        # 汇总的数据
        self.summarizeDialyDF = pd.DataFrame(summarize)

    def _summarizingDaily(self, optsv, navDF):
        """
        汇总一个主力连续的数据
        :param optsv:
        :param navDF: pd.DataFrame()
        :return: dict()
        """

        dic = {
            '品种': None,
            # u'首个交易日': None,
            # u'最后交易日': None,
            #
            # u'总交易日': None,
            # u'盈利交易日': None,
            # u'亏损交易日': None,

            # u'最大保证金占用': None,
            '总收益率': navDF['日净值'].iloc[-1],
            # u'最大回撤比': None,
            #
            # u'日均收益率': None,
            # u'收益标准差': None,
            # u'夏普率': None,
        }

        us, kwargs = self.parseOptsv(optsv)
        dic['品种'] = us
        # 参数
        dic.update(kwargs)
        return dic

    @staticmethod
    def parseOptsv(optsv):
        # 解析 optsv
        optsvSplit = optsv.split(',')
        # 品种
        us = underlyingSymbol = optsvSplit[0]
        # 参数
        argsStr = '{' + optsv[len(us) + 1:] + '}'
        return underlyingSymbol, json.loads(argsStr)

    def plot(self, title, series):
        """
        :param title:
        :param series: Series(index=DatetimeIndex())
        :return:
        """
        fig = plt.figure(figsize=(10, 16))

        subPlotNum = 6

        subplot = plt.subplot(subPlotNum, 1, 1)
        subplot.set_title(title)
        series.plot(legend=True)

    def rollingNav(self, underlyingSymbol, rollingWin='1BM', optWin=3):
        """
        对净值滚动窗口优化

        窗口周期参数 win 参考以下页面
        http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

        :param optWin: 优化窗口期，比如取最近3个自然月 3BM 的优化结果
        :param rollingWin: 滚动窗口期，比如按1个自然月，逐月滚动。将前3个自然月的最优结果应用到下一个自然月。
        :return:
        """
        us = underlyingSymbol
        # 日收益率df，index=date, column=optsv

        dic = {}
        for optsv, navDF in list(self.dailyReturnRateByOptsv.items()):
            if optsv.startswith('{},'.format(us)):
                wipedOut = navDF[navDF['日净值'] < 0.1]
                if wipedOut.shape[0] > 0:
                    # 爆仓了, 忽略改组参数
                    continue
                dic[optsv] = navDF['日收益率'] + 1

        # 生成日收益率df
        rollingWinDF = pd.DataFrame(dic)

        self.tmpDF = rollingWinDF

        self.rollingWinDF = rollingWinDF

        # 按滚动窗口期进行数据聚合，形成基本单位。按指定周期聚合聚合
        rollingWinDF = rollingWinDF.resample(rollingWin).prod()

        rollingWinDF = rollingWinDF.dropna()

        if 0 in rollingWinDF.shape:
            raise ValueError('没有任何数据')

        # 优化窗口
        optWinDF = rollingWinDF.rolling(optWin, min_periods=1).apply(lambda s: s.prod())

        # 转置
        dateIndex = optWinDF.index
        optWinDF = optWinDF.T
        rollingWinDF = rollingWinDF.T

        # 从优化窗口获取前一个最大值的索引
        date = optWinDF.columns[0]
        navSeries = optWinDF[date]
        preMaxIndex = navSeries.idxmax()
        preMax = navSeries.max()

        # 在下一个滚动窗口期中应用

        us, kwargs = self.parseOptsv(preMaxIndex)
        kwargs['收益率'] = 1
        kwargs['前最大收益'] = 0
        kwargs['optsv'] = preMaxIndex
        rollingNavList = [kwargs, ]

        for date in optWinDF.columns[1:]:
            # 从优化窗口获取前一个最大值的索引
            # 在下一个滚动窗口期中应用
            returnRateSeries = rollingWinDF[date]
            returnRate = returnRateSeries[preMaxIndex]
            # 解析这组参数
            us, kwargs = self.parseOptsv(preMaxIndex)
            kwargs['收益率'] = returnRate
            kwargs['前最大收益'] = preMax
            kwargs['optsv'] = preMaxIndex

            rollingNavList.append(kwargs)
            preMaxIndex = returnRateSeries.idxmax()
            preMax = returnRateSeries.max()

        # 生成 Series
        df = pd.DataFrame(rollingNavList, index=dateIndex)
        df['净值'] = df['收益率'].cumprod()
        return df

    def rollingDrawdown(self, underlyingSymbol, rollingWin='1BM', optWin=3):
        """
        对净值滚动窗口优化

        窗口周期参数 win 参考以下页面
        http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

        :param optWin: 优化窗口期，比如取最近3个自然月 3BM 的优化结果
        :param rollingWin: 滚动窗口期，比如按1个自然月，逐月滚动。将前3个自然月的最优结果应用到下一个自然月。
        :return:
        """

    def getMaxNav(self, underlyingSymbol):
        """
        根据品种获得净值最大值
        :param underlyingSymbol:
        :return:
        """

        self.summarizeDialyDF



if __name__ == '__main__':
    configPath = 'optimize.ini'
    with open(configPath, 'r') as f:
        config = configparser.SafeConfigParser()
        config.readfp(f)

    host, port, dbn, username, password = 'localhost', 30020, 'cta', 'vnpy', 'vnpy'
    client = pymongo.MongoClient(
        config.get('mongo', 'host'),
        config.getint('mongo', 'port'),
    )
    db = client[config.get('mongo', 'dbn')]
    db.authenticate(
        config.get('mongo', 'username'),
        config.get('mongo', 'password'),
    )
    resutlCol = db['btresult'].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

    cursor = resutlCol.find({'group': '开发调试'})
    a = AnalyseBacktesting([d for d in cursor])
