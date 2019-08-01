# coding:utf-8

import sys
import time
import logging
import json
from collections import OrderedDict
import pytz
from bson.codec_options import CodecOptions
from itertools import product
import configparser
import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
import arrow
import hisfursum.summarize

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

        self.config = configparser.ConfigParser()
        configPath = getJsonPath(optfile, __file__)

        # 指定回测参数的 collection
        with open(configPath, 'r') as f:
            self.config.read_file(f)

        with open(argFileName, 'r') as f:
            dic = json.load(f)

        self.param = dic['param']
        self.opts = OrderedDict(dic['opts'])

        # 只回测指定的品种
        self.includeUnderlyingSymbols = dic.get('includeUnderlyingSymbols')
        # self.includeUnderlyingSymbols = []
        self.excludeUnderlyingSymbols = dic.get('excludeUnderlyingSymbols')

        startTradingDay = dic['startTradingDay']
        self.startTradingDay = arrow.get(startTradingDay).datetime if startTradingDay else None
        if self.startTradingDay:
            self.log.info('startTradingDay {}'.format(self.startTradingDay))

        endTradingDay = dic['endTradingDay']
        self.endTradingDay = arrow.get(endTradingDay).datetime if endTradingDay else None
        if self.endTradingDay:
            self.log.info('endTradingDay {}'.format(self.endTradingDay))

        # 保存这一批回测参数的参数
        self.btinfo = dic.copy()
        self.btinfo['datetime'] = arrow.now().datetime

        self.log.info('group: {}'.format(self.param['group']))

        # 该组回测参数的参数
        self.setting = self.param.copy()
        self.setting['opts'] = self.opts

        # 回测模块的参数
        if not self.opts:
            err = '未设置需要优化的参数'
            self.log.critical(err)
            raise ValueError(err)

        self.param['opts'] = list(self.opts.keys())

        # 合约详情的 collection
        self.contractCol = None
        self.bar1dayCol = None

        # 生成的参数保存
        self.argCol = None
        # 该批参数的信息
        self.btinfoCol = None

    @property
    def group(self):
        return self.param['group']

    @property
    def name(self):
        return self.param['name']

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
        colName = self.config.get('contractMongo', 'collection')
        bar1dayColName = self.config.get('contractMongo', 'bar1dayCollection')

        client = MongoClient(
            host,
            port,
        )

        db = client[dbn]
        db.authenticate(username, password)

        self.contractCol = db[colName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        self.bar1dayCol = db[bar1dayColName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 将回测参数保存到数据库
        host = self.config.get('backtesting_mongo', 'host')
        port = self.config.getint('backtesting_mongo', 'port')
        username = self.config.get('backtesting_mongo', 'username')
        password = self.config.get('backtesting_mongo', 'password')
        dbn = self.config.get('backtesting_mongo', 'dbn')
        colName = self.config.get('backtesting_mongo', 'btarg')
        btinfoColName = self.config.get('backtesting_mongo', 'btinfo')

        self.log.info('即将到把回测参数导入到 {}:{}/{}/{}'.format(host, port, dbn, colName))
        # seconds = 3
        # while seconds > 0:
        #     self.log.info('{}'.format(seconds))
        #     seconds -= 1
        #     time.sleep(1)

        client = MongoClient(
            host,
            port,
        )
        db = client[dbn]
        db.authenticate(username, password)
        self.argCol = db[colName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))
        self.btinfoCol = db[btinfoColName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

    def start(self):
        """
        
        :return: 
        """
        self.dbConnect()

        # 检查参数
        self.checkArg()

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
        nameList = list(self.opts.keys())
        paramList = list(self.opts.values())

        # 使用迭代工具生产参数对组合
        productList = list(product(*paramList))

        # 把参数对组合打包到一个个字典组成的列表中
        settingList = []
        for p in productList:
            d = dict(list(zip(nameList, p)))
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
            'activeEndDate': {'$ne': None},
        }
        cursor = self.contractCol.find(sql)
        cursor = cursor.sort('activeEndDate', -1)

        contracts = [c for c in cursor]

        if self.includeUnderlyingSymbols:
            self.log.info('只回测品种：{}'.format(','.join(self.includeUnderlyingSymbols)))
            contracts = [c for c in contracts if c['underlyingSymbol'] in self.includeUnderlyingSymbols]
        else:
            self.log.info('回测全品种')
        if self.excludeUnderlyingSymbols:
            self.log.info('不回测品种：{}'.format(','.join(self.excludeUnderlyingSymbols)))
            contracts = [c for c in contracts if c['underlyingSymbol'] not in self.excludeUnderlyingSymbols]

        # 依然还在上市的品种
        onMarketUS = set()
        for c in contracts:
            if arrow.now() - c['activeEndDate'] < datetime.timedelta(days=40):
                # 一个月之内依然还活跃的品种
                onMarketUS.add(c['underlyingSymbol'])
            else:
                pass
        self.log.info('共 {} 上市品种'.format(len(onMarketUS)))

        if self.startTradingDay:
            contracts = [c for c in contracts if c['activeStartDate'] >= self.startTradingDay]
        if self.endTradingDay:
            contracts = [c for c in contracts if c['activeStartDate'] <= self.endTradingDay]

        sumarization = hisfursum.summarize.Summarization(self.bar1dayCol, self.contractCol)
        # 日成交量在 n 亿以上的品种
        n = 5
        minAmount = n * (10 ** 9)
        amountDF = sumarization.dailyAmountByActive()
        amountSeries = amountDF['amount']
        amountSeries = amountSeries[amountSeries > minAmount]

        # 一手保证金在 1万以下
        contractDF = sumarization.marginByActive()

        marginSeries = contractDF['margin']

        marginSeries = marginSeries[marginSeries < 10 ** 4]

        availbeContracts = []
        usSet = set()
        for c in contracts[:]:
            us = c['underlyingSymbol']
            if us not in onMarketUS:
                # 只取依然在上市的合约品种
                continue
            if us not in amountSeries.index:
                # 日成交额在10亿以上的
                continue
            # if us not in marginSeries.index:
            #     # 一手保证金在 1万以下
            #     continue
            usSet.add(us)
            availbeContracts.append(c)

        self.log.info('共 {} 个品种'.format(len(usSet)))

        self.btinfo['underlyingSymbols'] = list(usSet)
        self.btinfo['vtSymbols'] = [c['vtSymbol'] for c in availbeContracts]

        return availbeContracts

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

    def getFlt(self):
        return {'group': self.group, 'className': self.className}

    def saveBtinfo(self):
        """
        保存该批回测参数的信息
        :return:
        """
        assert isinstance(self.btinfoCol, Collection)
        flt = self.getFlt()

        self.btinfo.update(flt)

        # 替换
        self.btinfoCol.find_one_and_replace(flt, self.btinfo, upsert=True)

    def saveArgs(self, documents):
        """
        保存到数据库
        :return:
        """

        # 保存回测参数
        self.saveBtargs(documents)

        # 保存回测详情
        self.saveBtinfo()

    def saveBtargs(self, documents):

        count = len(documents)
        self.btinfo['amount'] = count
        if count > 10000:
            countStr = '{}万'.format(count / 10000.)
        else:
            countStr = count
        self.log.info('生成 {} 组参数'.format(countStr))

        # 删掉同名的参数组
        self.argCol.delete_many(self.getFlt())

        over = len(documents)
        once = 10000
        start, end = 0, once
        count = 0
        while end < over:
            self.argCol.insert_many(documents[start:end])
            count += once
            start, end = end, end + once

            self.log.info('{}/{} {}%'.format(count, over, round((count * 100. / over), 2)))
        self.argCol.insert_many(documents[start:])

    def checkArg(self):
        """
        检查参数设置是否正常
        :return:
        """
        # 是否已经有同名的参数组
        sql = {
            'group': self.group,
        }
        if self.group == 'test':
            # 测试用的组名直接删除
            self.btinfoCol.delete_one(sql)

        else:
            binfo = self.btinfoCol.find_one(sql)
            if binfo:
                raise ValueError('已经存在同名的参数组{}'.format(self.group))


if __name__ == '__main__':
    # 本机测试配置
    argFileName = 'opt_test.json'
    optfile = 'optimize.ini'

    # home 配置
    # argFileName = '/Users/lamter/workspace/SlaveO/svnpy/optization/opt_rb_oscillationDonchian.json'
    # optfile = 'optimizeHome.ini'

    logging.info('即将使用 {} 的配置'.format(optfile))
    time.sleep(5)

    b = BacktestingArg(argFileName, optfile)
    b.start()
