# coding:utf-8

import sys
import time
import logging
import json
from collections import OrderedDict
import pytz
from bson.codec_options import CodecOptions
from itertools import product
import ConfigParser
import datetime

from pymongo import MongoClient
import arrow

from vnpy.trader.vtFunction import getTempPath, getJsonPath


class BacktestingArg(object):
    """
    生成批量回测的参数
    """

    def __init__(self, argFileName, optfile='optimize.ini'):
        self.log = logging.getLogger()
        self.log.setLevel(logging.INFO)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        self.log.addHandler(sh)

        self.config = ConfigParser.SafeConfigParser()
        configPath = getJsonPath(optfile, __file__)

        # 指定回测参数的 collection
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        with open(argFileName, 'r') as f:
            dic = json.load(f)

        self.param = dic['param']
        self.opts = OrderedDict(dic['opts'])

        self.log.info(u'group: {}'.format(self.param['group']))

        # 回测模块的参数

        if not self.opts:
            err = u'未设置需要优化的参数'
            self.log.critical(err)
            raise ValueError(err)

        self.param['opts'] = list(self.opts.keys())

        # 合约详情的 collection
        self.contractCol = None
        # 生成的参数保存s
        self.argCol = None

    @property
    def group(self):
        return self.param['group']

    @property
    def className(self):
        return self.param['className']

    def dbConnect(self):
        # 获取合约信息
        host = self.config.get('contractMongo', 'host')
        port = self.config.getint('contractMongo', 'port')
        username = self.config.get('contractMongo', 'username')
        password = self.config.get('contractMongo', 'password')
        dbn = self.config.get('contractMongo', 'dbn')
        colName = self.config.get('contractMongo', 'collection')

        client = MongoClient(
            host,
            port,
        )

        db = client[dbn]
        db.authenticate(username, password)

        self.contractCol = db[colName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 将回测参数保存到数据库
        host = self.config.get('mongo', 'host')
        port = self.config.getint('mongo', 'port')
        username = self.config.get('mongo', 'username')
        password = self.config.get('mongo', 'password')
        dbn = self.config.get('mongo', 'dbn')
        colName = self.config.get('mongo', 'argCol')

        self.log.info(u'即将到把回测参数导入到 {}:{}/{}/{}'.format(host, port, dbn, colName))
        seconds = 3
        while seconds > 0:
            self.log.info('{}'.format(seconds))
            seconds -= 1
            time.sleep(1)

        client = MongoClient(
            host,
            port,
        )
        db = client[dbn]
        db.authenticate(username, password)
        self.argCol = db[colName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

    def start(self):
        """
        
        :return: 
        """
        self.dbConnect()

        # 生成优化参数组合
        strategyArgs = self.createStrategyArgsGroup()

        # 取出需要回测的合约
        contracts = self.getContractAvaible()

        # 生成最终用于回测的参数组合, 稍后保存到数据库
        documents = self.createBacktestingArgs(contracts, strategyArgs)

        # 保存
        self.saveArgs(documents)

    def createStrategyArgsGroup(self):
        # 参数名的列表
        nameList = self.opts.keys()
        paramList = self.opts.values()

        # 使用迭代工具生产参数对组合
        productList = list(product(*paramList))

        # 把参数对组合打包到一个个字典组成的列表中
        settingList = []
        for p in productList:
            d = dict(zip(nameList, p))
            settingList.append(d)

        # 策略参数组合
        keyList = list(self.opts.keys())
        keyList.sort()
        strategyArgs = []
        for s in settingList:
            d = self.param.copy()
            d.update(s)
            # 将待优化的参数组合成唯一索引
            d['optsv'] = ','.join(['"{}":{}'.format(n, d[n]) for n in keyList])
            strategyArgs.append(d)
            d['createTime'] = arrow.now().datetime

        return strategyArgs

    def getContractAvaible(self):
        """
        取出需要回测的合约
        :return:
        """
        # 取主力合约
        sql = {
            'activeStartDate': {'$ne': None},
            'activeEndDate': {'$ne': None}
        }

        cursor = self.contractCol.find(sql)
        cursor = cursor.sort('activeEndDate', -1)

        contracts = [c for c in cursor]

        # 依然还在上市的品种
        onMarketUS = set()
        for c in contracts:
            if arrow.now() - c['activeEndDate'] < datetime.timedelta(days=30):
                # 一个月之内依然还活跃的品种
                onMarketUS.add(c['underlyingSymbol'])
            else:
                pass
        self.log.info(u'共 {} 上市品种'.format(len(onMarketUS)))

        # 只取依然在上市的合约品种
        contracts = [c for c in contracts if c['underlyingSymbol'] in onMarketUS]
        return contracts

    def createBacktestingArgs(self, contracts, strategyArgs):
        """
        生成最终用于回测的参数组合,稍后保存到数据库
        :param contracts:
        :param strategyArgs:
        :return:
        """
        # 每个品种的回测参数
        documents = []
        for c in contracts:
            # # TODO 测试代码，先只测试螺纹
            if c['underlyingSymbol'] != 'hc':
                # if c['vtSymbol'] != 'hc1710':
                continue

            for a in strategyArgs:
                d = a.copy()
                d['optsv'] = '{},{}'.format(c['underlyingSymbol'], d['optsv'])
                d['vtSymbol'] = c['vtSymbol']
                d['activeStartDate'] = c['activeStartDate']
                d['activeEndDate'] = c['activeEndDate']
                d['priceTick'] = c['priceTick']
                d['size'] = c['size']
                d['underlyingSymbol'] = c['underlyingSymbol']
                documents.append(d)

        return documents

    def saveArgs(self, documents):
        """
        保存到数据库
        :return:
        """

        count = len(documents)
        if count > 1000:
            countStr = u'{}万'.format(count / 10000.)
        else:
            countStr = count
        self.log.info(u'生成 {} 组参数'.format(countStr))

        # 删掉同名的参数组
        self.argCol.delete_many({'group': self.group, 'className': self.className})
        self.argCol.insert_many(documents)


if __name__ == '__main__':
    argFileName = 'opt_CCI_SvtBollChannel.json'
    # 本机配置
    optfile = 'optimize.ini'

    # # home 配置
    # optfile = 'optimizeHome.ini'

    b = BacktestingArg(argFileName, optfile)
    b.start()
