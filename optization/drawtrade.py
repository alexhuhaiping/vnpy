# encoding: UTF-8

"""
绘制成交图
"""
import threading
import logging.config
import logging
import ConfigParser
import time
import multiprocessing

import pymongo
import arrow

import optserver
import backtestingarg
import optboss
import myplot.kline as mk

# # 清空本地数据库
config = ConfigParser.SafeConfigParser()
# configPath = 'localMongo.ini'
configPath = 'drawtrade.ini'
with open(configPath, 'r') as f:
    config.readfp(f)
#
host = config.get('backtesting_mongo', 'host')
port = config.getint('backtesting_mongo', 'port')
username = config.get('backtesting_mongo', 'username')
password = config.get('backtesting_mongo', 'password')
dbn = config.get('backtesting_mongo', 'dbn')
btinfo = config.get('backtesting_mongo', 'btinfo')
btarg = config.get('backtesting_mongo', 'btarg')
btresult = config.get('backtesting_mongo', 'btresult')

client = pymongo.MongoClient(host, port)
db = client[dbn]
db.authenticate(username, password)
btinfoCol = db[btinfo]
btargCol = db[btarg]
btresultCol = db[btresult]


# 运行清空命令
def clearCollection():
    btinfoCol.delete_many({})
    btargCol.delete_many({})
    btresultCol.delete_many({})


# 生成参数
logging.info(u'生成参数')
argFileName = 'opt_test.json'
optfile = 'drawtrade.ini'


def runArg():
    logging.info(u'即将使用 {} 的配置'.format(optfile))
    b = backtestingarg.BacktestingArg(argFileName, optfile)
    b.start()


# 运行批量回测
def runBackTesting():
    logging.info(u'启动批量回测服务端')
    server = optserver.OptimizeService(optfile)
    server.start()


# 读取日志配置文件
loggingConFile = 'logging.conf'


def runOptBoss():
    """
    运行回测算力
    :return:
    """
    logging.config.fileConfig(loggingConFile)
    time.sleep(2)
    logging.info(u'启动批量回测算力')
    server = optboss.WorkService(optfile)
    server.start()


if __name__ == '__main__':
    # 按照需要注释掉部分流程
    clearCollection() # 清空数据库
    runArg() # 生成参数

    # 批量回测 ==>
    child_runBackTesting = multiprocessing.Process(target=runBackTesting)
    child_runBackTesting.start()
    child_runOptBoss = multiprocessing.Process(target=runOptBoss)
    child_runOptBoss.start()

    btInfoDic = btinfoCol.find_one({})
    while True:
        time.sleep(1)
        cursor = btresultCol.find()
        count = cursor.count()
        print('result {}'.format(count))
        if btInfoDic['amount'] == count:
            # 已经全部回测完毕
            break
        cursor.close()
    child_runBackTesting.terminate()
    child_runOptBoss.terminate()
    # <== 批量回测完成

    bars = mk.qryBarsMongoDB(
        underlyingSymbol='AP',

        # 截取回测始末日期，注释掉的话默认取全部主力日期
        # startTradingDay=arrow.get('2019-01-07 00:00:00+08:00').datetime,
        # endTradingDay=arrow.get('2018-03-10 00:00:00+08:00').datetime,

        host=config.get('ctp_mongo', 'host'), port=config.getint('ctp_mongo', 'port'),
        dbn=config.get('ctp_mongo', 'dbn'), collection=config.get('ctp_mongo', 'collection'),
        username=config.get('ctp_mongo', 'username'), password=config.get('ctp_mongo', 'password'),
    )

    originTrl = mk.qryBtresultMongoDB(
        underlyingSymbol='AP',
        optsv='AP,"barXmin":20,"longBar":10,"n":1',
        host=host, port=port, dbn=dbn, collection=btresult, username=username, password=password,
    )

    tradeOnKlinePlot = mk.tradeOnKLine('1T', bars, originTrl, width=3000, height=1350)
    tradeOnKlinePlot.render('/Users/lamter/Downloads/回测成交图.html')
