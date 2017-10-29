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

        self.navDf = None

    def calNav(self):
        """
        分组计算净值
        :return:
        """
        self.df[u'总收益率'].groupby('barPeriod')


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
