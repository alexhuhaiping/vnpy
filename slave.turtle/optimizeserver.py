# coding:utf-8

import pytz
from bson.codec_options import CodecOptions
import ConfigParser
import time
import logging.config
import multiprocessing
import signal
import datetime

import arrow
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING

from vnpy.trader.vtFunction import getTempPath, getJsonPath
from runBacktesting import runBacktesting

# 读取日志配置文件
loggingConFile = 'logging.conf'
loggingConFile = getJsonPath(loggingConFile, __file__)
logging.config.fileConfig(loggingConFile)


class OptimizeService(object):
    """批量回测的服务
    1. 监控 collection： backtesting 中的条目
    2. 运行其中参数进行回测
    3. 将回测结果保存到 backtesting
    """

    def __init__(self, config=None):
        self.cpuCount = multiprocessing.cpu_count()

        self.localzone = pytz.timezone('Asia/Shanghai')

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)

        self.config = ConfigParser.SafeConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        # # 数据库链接
        self.client = pymongo.MongoClient(
            host=self.config.get('mongo', 'host'),
            port=self.config.getint('mongo', 'port'),
        )

        self.db = self.client[self.config.get('mongo', 'dbn')]

        self.db.authenticate(
            self.config.get('mongo', 'username'),
            self.config.get('mongo', 'password'),
        )

        # 回测任务参数队列
        self.argCol = self.db[self.config.get('mongo', 'argCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        self.resultCol = self.db[self.config.get('mongo', 'resultCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 初始化索引
        self.initContractCollection()

        # 进程池
        # self.pool = multiprocessing.Pool(self.cpuCount)

    def initContractCollection(self):
        # 需要建立的索引
        indexSymbol = IndexModel([('vtSymbol', ASCENDING)], name='vtSymbol', background=True)
        indexClassName = IndexModel([('className', ASCENDING)], name='className', background=True)
        indexGroup = IndexModel([('group', ASCENDING)], name='group', background=True)
        indexUnderlyingSymbol = IndexModel([('underlyingSymbol', DESCENDING)], name='underlyingSymbol', background=True)

        indexes = [indexSymbol, indexClassName, indexGroup, indexUnderlyingSymbol]

        self._initCollectionIndex(self.argCol, indexes)
        self._initCollectionIndex(self.resultCol, indexes)

    def _initCollectionIndex(self, col, indexes):
        """
        初始化分钟线的 collection
        :return:
        """

        # 检查索引
        try:
            indexInformation = col.index_information()
            for indexModel in indexes:
                if indexModel.document['name'] not in indexInformation:
                    col.create_indexes(
                        [
                            indexModel,
                        ],
                    )
        except OperationFailure:
            # 有索引
            col.create_indexes(indexes)

    def start(self):
        self._active = True
        self._run()

    def _run(self):
        while self._active:
            self.run()

        self.exit()

    def shutdown(self, signalnum, frame):
        self._active = False

    def exit(self):
        """

        :return:
        """
        # self.pool.close()
        # self.pool.join()

    def run(self):
        # 检查 colleciton 中是否有新的回测任务
        cursor = self.argCol.find()

        if cursor.count() == 0:
            # 没有任何任务
            return

        # 优先回测最近的品种
        cursor = cursor.sort('activeEndDate', -1)

        # 每次取出前1000条来进行回测
        settingList = [s for s in cursor.limit(1000)]
        for setting in settingList:
            vtSymbol = setting['vtSymbol']
            engine = runBacktesting(vtSymbol, setting, isShowFig=False)
            setting.update(engine.dailyResult)
            _id = setting.pop('_id')

            # 将回测结果进行保存
            setting['datetime'] = arrow.now().datetime

            # 将 datetime.date 转化为 datetime.datetime
            for k, v in list(setting.items()):
                if isinstance(v, datetime.date):
                    v = datetime.datetime.combine(v, datetime.time())
                    v = self.localzone.localize(v)
                    setting[k] = v

            self.resultCol.insert_one(setting)

            # 删除掉任务
            # self.argCol.delete_one({'_id': _id})

        # 查出进程数那么多的任务

        # 启用子进程运行

        # 将返回的结果存库

        # 删掉完成的任务

        self._active = False
        time.sleep(3)


def optimize():
    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, lambda signalnum, frame: None)


if __name__ == '__main__':
    server = OptimizeService()
    server.start()
