# encoding: utf-8
import sys
import os
import json
import datetime
import re
import traceback
import logging

import pandas as pd
import pymongo
import tradingtime


class Washer(object):
    """
    清洗数据
    """

    # 处理几天内的数据
    PRE_DAYS = 2

    objectNameFilter = re.compile(r'^\D*').match

    def __init__(self):
        try:
            VT_setting = sys.argv[1]
            if not os.path.exists(VT_setting):
                raise IOError('VT_setting {} not found'.format(VT_setting))
        except:
            VT_setting = './tmp/VT_setting.json'

        # 日志
        self.logger = logging.getLogger('drwasher')
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s[line:%(lineno)d] - %(message)s'))
        self.logger.addHandler(sh)

        with open(VT_setting, 'r') as f:
            self.VT_setting = json.load(f)

        self.mongodUrl = 'mongodb://{mongoUsername}:{mongoPassword}@{mongoHost}:{mongoPort}/{dbn}?authMechanism=SCRAM-SHA-1'.format(
            **self.VT_setting)

        self.today = datetime.datetime.now()
        self.preDate = self.today - datetime.timedelta(days=self.PRE_DAYS)

        self.dbClient = None  # pymongo.MongoClient()
        self.db = None  # pymongo.MongoClient.database()
        self.contracts = None  # pd.DataFrame()

        self._tickData = None

    def do(self):
        """
        数据清洗流程
        :return:
        """

        # 建立 pymongo 链接
        self.dbConnect()

        # initContracts
        self.initContracts()

        # 检查索引
        self.checkIndexes()

        for index, tickInfo in self.contracts.iterrows():
            if __debug__:
                testSymbol = 'ag1712'
                if tickInfo.vtSymbol != testSymbol:
                    continue
            # 有索引时的操作
            self._tickData = self.getTickData(tickInfo)

            if self._tickData.shape[0] == 0:
                continue

            if not tickInfo.hasIndex:
                # 没有索引，需要去重
                self._tickData = self.dropDunplicateTickData(self._tickData)

            # TODO 去掉非交易时间的数据
            # self._tickData = self.clearNotInTradetime(tickInfo, tickData)

            # TODO 添加 ActionDay 和 TradeDay

            if not tickInfo.hasIndex:
                pass
                # TODO 创建索引

            if __debug__:
                if tickInfo.vtSymbol != testSymbol:
                    break

    def __del__(self):
        try:
            self.logger.info('close mongodb ...')
            self.dbClient.close()
        except:
            self.logger.warning('mongdb close error ...')

    def dbConnect(self):
        """
        建立链接
        :return:
        """

        # 设置MongoDB操作的超时时间为0.5秒
        self.dbClient = pymongo.MongoClient(self.mongodUrl, connectTimeoutMS=500)
        self.dbClient.server_info()
        self.db = self.dbClient[self.VT_setting['dbn']]

    def initContracts(self):
        """
        初始化合约的基本信息
        :return:
        """

        # 获取这几天来的合约信息
        sql = {
            'ActionDay': {
                '$gte': self.preDate.strftime('%Y%m%d')
            }
        }

        cursor = self.db.contract.find(sql)
        self.contracts = pd.DataFrame([i for i in cursor])

        # 添加对应的数据库名
        self.contracts[self.tickcol] = self.contracts.vtSymbol.map(lambda x: '{}_tick'.format(x))

        # 增加对应的商品名，如螺纹钢就是 'rb'
        self.contracts['objectName'] = self.contracts.vtSymbol.map(self.objectNameFilter)

    def checkIndexes(self):
        """
        检查索引
        :return:
        """
        db = self.db
        isHasIndex = []
        w = self

        def checkHasIndex(db, t, w):
            c = db[t]
            index_infomation = dict(c.index_information())
            for k in index_infomation.keys():
                if w.tickIndexName in k:
                    # 存在索引
                    return True
            else:
                return False

        for t in w.contracts.tickcol:
            # 是否已经有索引了
            isHasIndex.append(checkHasIndex(db, t, w))

        w.contracts['hasIndex'] = isHasIndex

    @property
    def tickcol(self):
        return 'tickcol'

    @property
    def tickIndexName(self):
        return 'datetime'

    def dropDunplicateTickData(self, tickData):
        """
        去掉重复的 tick
        :param tickData: pd.DataFrame()
        :return: pd.DataFrame()
        """
        assert isinstance(tickData, pd.DataFrame)

        return tickData.drop_duplicates(self.tickIndexName)

    def getTickData(self, tickInfo):
        """

        :return: pd.DataFrame()
        """

        col = self.db[tickInfo.tickcol]
        sql = {
            'date': {
                '$gte': self.preDate.strftime('%Y%m%d')
            }
        }

        cursor = col.find(sql)
        return pd.DataFrame([i for i in cursor])

    def clearNotInTradetime(self, tickInfo, tickData):
        """
        去掉非交易时间的数据
        :param tickInfo: pd.DataFrame()
        :param tickData: pd.DataFrame()
        :return:
        """
        # TODO 找出交易时间
        tradingtime.get_tradingtime_by_status()

        # TODO 剔除非交易时间段的数据


if __name__ == '__main__':
    w = Washer()
