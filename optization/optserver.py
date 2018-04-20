# coding:utf-8
import os

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import Queue as queue
except ImportError:
    import queue
from threading import Event, Thread
import pytz
from bson.codec_options import CodecOptions
import ConfigParser
import logging.config
import multiprocessing
import signal
import pymongo.errors

from slavem import Reporter
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING

from vnpy.trader.vtFunction import getTempPath, getJsonPath
import optweb

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

        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        self.gitHash = r.read().strip('\n')

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

        self.salt = self.config.get('web', 'salt')

        # 任务队列
        self.tasksQueue = queue.Queue(5)

        self.resultQueue = queue.Queue(5)

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
        self.webForever = optweb.ServerThread(self)

        self.createTaskForever = Thread(name=u'createTask', target=self.__createTaskForever)
        self.saveResultForever = Thread(name=u'saveResult', target=self.__saveResultForever)

        # 初始化索引
        self.initContractCollection()

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)

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

        # 对照已经完成回测的结果
        self.checkResult()

        self.webForever.start()
        self.createTaskForever.start()
        self.saveResultForever.start()

        self.run()

    def checkResult(self):
        """
        检查已经完成任务，从任务collection中剔除
        :return:
        """
        self.log.info(u'开始核对已经完成的回测任务')
        cursor = self.argCol.find({}, {}, no_cursor_timeout=True)
        if cursor.count() == 0:
            self.log.info(u'没有需要核对的回测任务')
            return

        argsIDs = {d['_id'] for d in cursor}

        cursor = self.resultCol.find({}, {}, no_cursor_timeout=True)
        resultIDs = {d['_id'] for d in cursor}

        # 对比回测
        finishIDs = argsIDs & resultIDs

        self.log.info(u'已经完成任务 {} of {}'.format(len(finishIDs), len(argsIDs)))

        # 删除已经完成的回测参数
        for _id in finishIDs:
            self.argCol.delete_many({'_id': _id})

    def run(self):
        while not self.stoped.wait(30):
            self.slavemReport.heartBeat()
        self.slavemReport.endHeartBeat()

    def shutdown(self, signalnum, frame):
        self.stop()

    def stop(self):
        self.log.info(u'关闭')
        self.stoped.set()

    def exit(self):
        self.clearTaskQueue()
        self.clearResultQueue()

    def accpetResult(self, result):
        """
        返回的结束
        :param result:
        :return:
        """
        self.resultQueue.put(result)

    def __createTaskForever(self):
        self._createTaskForever()

        while not self.stoped.wait(60):
            self.checkResult()
            self._createTaskForever()

    def _createTaskForever(self):
        """
        从队列中创建任务
        :return:
        """
        # 检查 colleciton 中是否有新的回测任务
        cursor = self.argCol.find(no_cursor_timeout=True)

        count = cursor.count()
        if count == 0:
            # 没有任何任务
            self.log.info(u'没有回测任务')
            return
        else:
            self.log.info(u'即将开始回测任务 {} 个'.format(count))

        # 优先回测最近的品种
        cursor = cursor.sort([
            ('activeEndDate', -1),
            ('vtSymbol', -1)
        ])

        for setting in cursor:
            # 持续尝试塞入
            while not self.stoped.wait(0):
                try:
                    self.tasksQueue.put(setting, timeout=1)
                    break
                except queue.Full:
                    pass

    def __saveResultForever(self):
        self._saveResultForever()

        while not self.stoped.wait(5):
            self._saveResultForever()

    def _saveResultForever(self):
        """

        :return:
        """
        self.results = []
        while not self.stoped.wait(0):
            try:
                r = self.resultQueue.get(timeout=3)

                if r[u'总交易次数'] < 1:
                    # 总交易次数太少的不保存
                    continue
                self.results.append(r)
                if len(self.results) >= 100:
                    self.insertResult(self.results)
                    self.results = []
            except queue.Empty:
                if self.results:
                    self.insertResult(self.results)
                    self.results = []

    def insertResult(self, results):
        try:
            self.resultCol.insert_many(self.results)
        except pymongo.errors.BulkWriteError:
            # 出现重复的 _id
            for r in results:
                try:
                    self.resultCol.update_one({'_id': r['_id']}, r, upsert=True)
                except pymongo.errors.DuplicateKeyError:
                    pass


if __name__ == '__main__':
    server = OptimizeService()
    server.start()
    server.run()
