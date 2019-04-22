# coding:utf-8
import os
import traceback
import time

import pickle

import queue

from threading import Event, Thread
import pytz
from bson.codec_options import CodecOptions
import configparser
import logging.config
import multiprocessing
import signal
import pymongo.errors
import requests

from slavem import Reporter
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING

from vnpy.trader.vtFunction import getTempPath, getJsonPath
import optweb
import optcomment


class OptimizeService(object):
    """批量回测的服务
    1. 监控 collection： backtesting 中的条目
    2. 运行其中参数进行回测
    3. 将回测结果保存到 backtesting
    """

    SIG_STOP = 'close_service'

    def __init__(self, config=None):

        # 读取日志配置文件
        loggingConFile = 'logging.conf'
        loggingConFile = getJsonPath(loggingConFile, __file__)
        logging.config.fileConfig(loggingConFile)

        self.log = logging.getLogger()
        self.webLog = logging.getLogger('web')

        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        self.gitHash = r.read().strip('\n')

        self.cpuCount = multiprocessing.cpu_count()

        # self.cpuCount = 1
        self.localzone = pytz.timezone('Asia/Shanghai')

        self.config = configparser.ConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.read_file(f)

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

        self.salt = self.config.get('web', 'salt').encode()

        # 数据库链接
        self.client = pymongo.MongoClient(
            host=self.config.get('backtesting_mongo', 'host'),
            port=self.config.getint('backtesting_mongo', 'port'),
        )

        self.db = self.client[self.config.get('backtesting_mongo', 'dbn')]

        self.db.authenticate(
            self.config.get('backtesting_mongo', 'username'),
            self.config.get('backtesting_mongo', 'password'),
        )

        # 回测任务参数
        self.argCol = self.db[self.config.get('backtesting_mongo', 'btarg')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 回测结果
        self.resultCol = self.db[self.config.get('backtesting_mongo', 'btresult')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 任务队列
        self.pid = os.getpid()
        self.tasksQueue = multiprocessing.Queue(5)
        # 尽量避免结果堵塞
        self.resultQueue = multiprocessing.Queue(10000)
        self.logQueue = multiprocessing.Queue()
        self.stoped = Event()
        self.stoped.set()

        # 创建任务
        self.webForever = multiprocessing.Process(
            target=optweb.run_app,
            args=(self.pid, self.gitHash, self.salt, self.logQueue, self.tasksQueue, self.resultQueue)
        )

        self.createTaskForever = Thread(name='createTask', target=self.__createTaskForever)
        self.saveResultForever = Thread(name='saveResult', target=self.__saveResultForever)
        self.weblogForever = Thread(name='logWeb', target=self.__logWebForever)

        # 初始化索引
        self.initContractCollection()

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)
            signal.siginterrupt(sig, False)

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
        self.log.warning('分布式回测启动')
        self.stoped.clear()
        self.webForever.start()

        # 清除队列信息
        self.clearTaskQueue()
        self.clearResultQueue()

        # 对照已经完成回测的结果
        self.checkResult()

        # 开始心跳
        self.slavemReport.heartBeat()

        self.createTaskForever.start()
        self.saveResultForever.start()
        self.weblogForever.start()

        self.run()

    def checkResult(self):
        """
        检查已经完成任务，从任务collection中剔除
        :return:
        """
        self.log.info('开始核对已经完成的回测任务')
        dic = self.resultCol.find_one({}, {}, no_cursor_timeout=True)
        if not dic:
            self.log.info('不需要核对回测任务')
            return

        dic = self.argCol.find_one({}, {}, no_cursor_timeout=True)
        if not dic:
            self.log.info('没有需要核对的回测任务')
            return

        cursor = self.argCol.find({}, {}, no_cursor_timeout=True)
        argsIDs = {d['_id'] for d in cursor}

        cursor = self.resultCol.find({}, {}, no_cursor_timeout=True)
        resultIDs = {d['_id'] for d in cursor}

        # 对比回测
        finishIDs = argsIDs & resultIDs

        self.log.info('已经完成任务 {} of {}'.format(len(finishIDs), len(argsIDs)))

        # 删除已经完成的回测参数
        for _id in finishIDs:
            self.argCol.delete_many({'_id': _id})

    def run(self):
        originInterval = 30
        errInterval = 10
        interval = originInterval
        while not self.stoped.wait(interval):
            # 检查信号
            try:
                self.beatWeb()
                interval = originInterval
            except Exception:
                self.log.error(traceback.format_exc())
                #  长时间异常，重启web服务
                self.newWebForever()
                interval = errInterval
                continue
                # self.slavemReport.heartBeat()

        self.slavemReport.endHeartBeat()

    def newWebForever(self):
        self.log.warning('尝试重启 web 子进程')
        self.webForever.join(2)
        self.webForever.terminate()

        time.sleep(10)

        self.webForever = multiprocessing.Process(
            target=optweb.run_app,
            args=(self.pid, self.gitHash, self.salt, self.logQueue, self.tasksQueue, self.resultQueue)
        )
        testUrl = self.getTestUrl()
        while not self.stoped.wait(1):
            try:
                if requests.get(testUrl, timeout=1).status_code == 200:
                    self.log.warning('web尚未完全关闭')
                    continue
            except Exception:
                break
        self.log.warning('重启 web 子进程完毕')
        self.webForever.start()

    def beatWeb(self):
        """
        定时查看 web 服务是否正常，如果异常则重启web服务
        :return:
        """
        url = self.getBeatUrl()
        data = optcomment.saltedByHash(b'test', self.salt)
        url += '/{}'.format(data)
        r = requests.get(url, timeout=3)
        if r.status_code != 200:
            raise ValueError('status:{}'.format(r.status_code))

    def getBeatUrl(self):
        return self.config.get('web', 'url') + '/beat'

    def getTestUrl(self):
        return self.config.get('web', 'url') + '/test'

    def shutdown(self, signalnum, frame):
        self.stop()

    def stop(self):
        self.log.info('关闭')
        self.webForever.join(5)
        self.webForever.terminate()
        self.stoped.set()

    def exit(self):
        self.clearTaskQueue()
        self.clearResultQueue()

    def __createTaskForever(self):
        self.log.info('开始生成任务')
        self._createTaskForever()

        while not self.stoped.wait(60):
            self.checkResult()
            self._createTaskForever()

        self.log.info('生成任务停止')

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
            self.log.info('没有回测任务')
            # 关闭心跳
            self.slavemReport.heartBeat()
            self.slavemReport.endHeartBeat()
            return
        else:
            self.log.info('即将开始回测任务 {} 个'.format(count))

        for setting in cursor:
            # 持续尝试塞入
            while not self.stoped.wait(0):
                try:
                    self.tasksQueue.put(setting, timeout=1)
                    break
                except queue.Full:
                    pass

    def __saveResultForever(self):
        self.log.info('开始保存结果')
        self._saveResultForever()

        while not self.stoped.wait(5):
            self._saveResultForever()

        self.log.info('保存结果停止')

    def __logWebForever(self):
        self.log.info('weblog 开始')
        while not self.stoped.wait(0):
            try:
                level, text = self.logQueue.get(timeout=1)
                func = getattr(self.webLog, level)
                func(text)
            except queue.Empty:
                pass

        self.log.info('weblog 停止')

    def _saveResultForever(self):
        """

        :return:
        """
        self.results = []
        begin = time.time()
        # while not self.stoped.wait(0):
        while not self.stoped.wait(1):
            try:
                # r = self.resultQueue.get(timeout=1)
                r = self.resultQueue.get_nowait()
                if not r:
                    if self.results:
                        self.insertResult(self.results)
                        self.results = []
                    continue

                now = time.time()
                if now - begin > 30:
                    begin = now
                    # 没有拿到数据不进行心跳
                    # 距离上次心跳已经30秒
                    self.slavemReport.heartBeat()

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
            # try:
            #     for k, v in self.results[0].items():
            #         print('{}\t{}\t{}'.format(k,type(v), v))
            #         self.resultCol.insert_one({k:v})
            # except Exception:
            #     print('{}\t{}\t{}'.format(k,type(v), v))
            #     time.sleep(1)
            #     raise
            self.resultCol.insert_many(self.results)
        except pymongo.errors.BulkWriteError:
            # 出现重复的 _id
            for r in results:
                try:
                    self.resultCol.update_one({'_id': r['_id']}, {'$set': r}, upsert=True)
                except pymongo.errors.DuplicateKeyError:
                    pass


if __name__ == '__main__':
    optfile = 'optimize.ini'
    server = OptimizeService(optfile)
    server.start()
