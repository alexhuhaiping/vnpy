# coding:utf-8

import os

from Queue import Empty, Full, Queue
from threading import Event, Thread
import pytz
from bson.codec_options import CodecOptions
import ConfigParser
import time
import logging.config
import multiprocessing
import signal
import datetime
import traceback

from slavem import Reporter
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

    SIG_STOP = 'close_service'

    def __init__(self, config=None):
        self.log = logging.getLogger('ctabacktesting')

        self.cpuCount = multiprocessing.cpu_count()

        # self.cpuCount = 1
        self.localzone = pytz.timezone('Asia/Shanghai')

        self.config = ConfigParser.SafeConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        # slavem监控
        self.slavemReport = Reporter(
            self.config.get('slavem', 'name'),
            self.config.get('slavem', 'type'),
            self.config.get('slavem', 'host'),
            self.config.getint('slavem', 'port'),
            self.config.get('slavem', 'dbn'),
            self.config.get('slavem', 'username'),
            self.config.get('slavem', 'password'),
            self.config.get('slavem', 'localhost'),
        )

        # 任务队列
        self.tasksQueue = Queue(100)

        self.finishTasksIDSet = set()
        self.finishSettingIDQueue = Queue()

        self.stoped = Event()
        self.stoped.set()

        # 数据库链接
        self.client = pymongo.MongoClient(
            host=self.config.get('mongo', 'host'),
            port=self.config.getint('mongo', 'port'),
        )

        self.db = self.client[self.config.get('mongo', 'dbn')]

        self.db.authenticate(
            self.config.get('mongo', 'username'),
            self.config.get('mongo', 'password'),
        )

        # 回测任务参数
        self.argCol = self.db[self.config.get('mongo', 'argCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 回测结果
        self.resultCol = self.db[self.config.get('mongo', 'resultCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 创建任务
        self.createTaskForever = Thread(name=u'创建任务', target=self.__createTaskForever)

        # 初始化索引
        self.initContractCollection()

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)

    def logout(self):
        self.log.info('启动')
        while self.logActive:
            try:
                name, level, msg = self.logQueue.get(timeout=1)
            except Empty:
                continue

            log = self.logs[name]
            func = getattr(log, level)
            func(msg)
        self.log.info('结束')

    def initContractCollection(self):
        # 需要建立的索引
        indexSymbol = IndexModel([('vtSymbol', ASCENDING)], name='vtSymbol', background=True)
        indexClassName = IndexModel([('className', ASCENDING)], name='className', background=True)
        indexGroup = IndexModel([('group', ASCENDING)], name='group', background=True)
        indexUnderlyingSymbol = IndexModel([('underlyingSymbol', DESCENDING)], name='underlyingSymbol', background=True)
        indexOptsv = IndexModel([('optsv', DESCENDING)], name='v', background=True)

        indexes = [indexSymbol, indexClassName, indexGroup, indexUnderlyingSymbol, indexOptsv]

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

    def clearTaskQueue(self):
        # TODO clearTaskQueue
        pass

    def clearResultQueue(self):
        # TODO clearResultQueue
        pass

    def start(self):
        self.stoped.clear()

        # 清除队列信息
        self.clearTaskQueue()
        self.clearResultQueue()

        # TODO 对照已经完成回测的结果
        self.checkResult()

    def checkResult(self):
        """

        :return:
        """
        cursor = self.argCol.find({}, {}, no_cursor_timeout=True)
        if cursor.count() == 0:
            return
        argsIDs = {d['_id'] for d in cursor}

        cursor = self.resultCol.find({}, {}, no_cursor_timeout=True)
        resultIDs = {d['_id'] for d in cursor}

        # 对比回测
        finishIDs = argsIDs & resultIDs

        # 删除已经完成的回测参数
        for _id in finishIDs:
            self.argCol.delete_many({'_id': _id})

    def run(self):
        self.slavemReport.heartBeat()
        self.createTaskForever.start()

        while not self.stoped.wait(1):
            pass
        self.slavemReport.endHeartBeat()

    def shutdown(self, signalnum, frame):
        self.stop()

    def stop(self):
        self.log.info(u'关闭')
        self.stoped.set()

    def exit(self):
        self.clearTaskQueue()
        self.clearResultQueue()

    def __createTaskForever(self):
        self._createTaskForever()

        while not self.stoped.wait(60):
            self._createTaskForever()
            self.checkResult()

    def _createTaskForever(self):
        """
        从队列中创建任务
        :return:
        """
        # 检查 colleciton 中是否有新的回测任务
        cursor = self.argCol.find(no_cursor_timeout=True)

        if cursor.count() == 0:
            # 没有任何任务
            return

        # 优先回测最近的品种
        cursor = cursor.sort([
            ('activeEndDate', -1),
            ('vtSymbol', -1)
        ])

        for setting in cursor:
            # 持续尝试塞入
            while not self.stoped.wait(0.01):
                try:
                    self.tasksQueue.put(setting, timeout=1)
                except Full:
                    pass


if __name__ == '__main__':
    server = OptimizeService()
    server.start()
    server.run()
