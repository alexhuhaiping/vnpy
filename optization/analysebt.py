# coding:utf-8

import pandas as pd
import pytz
from bson.codec_options import CodecOptions
import pymongo
import ConfigParser


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
        self.dailyReturnRateByOptsv = {}  # {'optsv': pd.DataFrame()}

        self.navDf = None

    def init(self):
        """
        初始化数据
        :return:
        """
        self.calNav()

    def calNav(self):
        """
        分组计算净值
        :return:
        """
        # 取出需要计算的数据
        nav = self.df.sort_values(u'最后交易日')
        cols = [u'日收益率曲线', u'optsv', u'总收益率', 'underlyingSymbol', 'activeEndDate']
        nav = nav[cols].copy()
        nav[u'总收益率'] += 1

        # 根据品种分组
        group = nav.groupby('optsv')

        optsvList = group.indices.keys()

        # 取得每一组连续的数据
        for optsv in optsvList:
            consisDF = group.get_group(optsv)
            # 排序
            consisDF.sort_values('activeEndDate')
            dailyReturnRateList = [0]

            # 取出每日收益率
            consisDF[u'日收益率曲线'].apply(lambda x: dailyReturnRateList.extend(x))
            self.calDaily(optsv, dailyReturnRateList)

            # 生成日收益和日净值

    def calDaily(self, optsv, dailyReturnRateList):
        """
        据此计算其他数值汇总参数
        :return:
        """

        drrList = dailyReturnRateList

        df = pd.DataFrame({u'日收益率': drrList})
        df[u'日净值'] = df[u'日收益率'] + 1
        df[u'日净值'] = df[u'日净值'].cumprod()

        self.dailyReturnRateByOptsv[optsv] = df

        if __debug__:
            self.debugDF = df


if __name__ == '__main__':
    configPath = 'optimize.ini'
    with open(configPath, 'r') as f:
        config = ConfigParser.SafeConfigParser()
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

    cursor = resutlCol.find({'group': u'开发调试'})
    a = AnalyseBacktesting([d for d in cursor])
