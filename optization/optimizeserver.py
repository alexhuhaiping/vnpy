# coding:utf-8

import os
from Queue import Empty, Full
import threading
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
        self.settingQueue = multiprocessing.Queue(self.cpuCount)
        self.logQueue = multiprocessing.Queue()
        self.stopQueue = multiprocessing.Queue()
        self.logs = {}
        self.finishTasksIDSet = set()
        self.finishSettingIDQueue = multiprocessing.Queue()

        self.active = False

        # 进程队列
        self.wokers = []
        for i in range(self.cpuCount):
            name = 'wodker_{}'.format(i)
            self.logs[name] = logging.getLogger(name)

            w = Optimization(self.settingQueue, self.logQueue, self.stopQueue, self.config, self.finishSettingIDQueue,
                             name=name)
            self.wokers.append(w)
            self.log.info('woker {}'.format(name))

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

        # 回测任务参数队列
        self.argCol = self.db[self.config.get('mongo', 'argCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        self.resultCol = self.db[self.config.get('mongo', 'resultCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 初始化索引
        self.initContractCollection()

        self.threadLog = threading.Thread(target=self.logout)
        self.logActive = True

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

    def start(self):
        self.active = True

        self.threadLog.start()

        for w in self.wokers:
            w.start()
        try:
            self.run()
        except:
            err = traceback.format_exc()
            self.log.info('error', err)
            self.stopQueue.put(self.SIG_STOP)

    def run(self):
        try:
            self.slavemReport.heartBeat()
            while self.active:
                try:
                    self._run()
                except Empty:
                    continue
        except:
            err = traceback.format_exc()
            self.log.critical(err)
            raise

        self.exit()

    def shutdown(self, signalnum, frame):
        self.stop()

    def stop(self):
        self.log.info('关闭')
        self.active = False
        for i in range(self.cpuCount * 10):
            self.stopQueue.put(self.SIG_STOP)

    def exit(self):
        """

        :return:
        """

        for w in self.wokers:
            self.log.info('等待 {} 结束'.format(w.name))
            w.join()

        if self.threadLog.isAlive():
            self.logActive = False
            self.threadLog.join()

        self.slavemReport.endHeartBeat()

    def _run(self):

        # 检查 colleciton 中是否有新的回测任务
        cursor = self.argCol.find()

        if cursor.count() == 0:
            # 没有任何任务
            time.sleep(3)
            return

        # 优先回测最近的品种
        cursor = cursor.sort('activeEndDate', -1)

        # 每次取出前1000条来进行回测
        limitNum = 1000
        count = 0
        total = cursor.count()
        settingList = [s for s in cursor.limit(limitNum)]
        for setting in settingList:
            self.log.info(u'{} / {}'.format(count, total))
            self.finishTasksIDSet.add(setting['_id'])
            # 一次最多只能放8个
            while self.active:
                try:
                    self.settingQueue.put(setting, timeout=1)
                    break
                except Full:
                    pass

        while self.active and self.finishTasksIDSet:
            try:
                _id = self.finishSettingIDQueue.get(timeout=1)
                self.finishTasksIDSet.remove(_id)
            except (Empty, KeyError):
                pass


class Optimization(multiprocessing.Process):
    """
    执行优化的子进程
    """
    SIG_STOP = 'close_service'

    def __init__(self, settingQueue, logQueue, stopQueue, config, finishSettingIDQueue, *args, **kwargs):
        super(Optimization, self).__init__(*args, **kwargs)

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            # signal.signal(sig, lambda signalnum, frame: None)
            signal.signal(sig, self._shutdown)

        self.localzone = pytz.timezone('Asia/Shanghai')

        self.config = config
        self.settingQueue = settingQueue
        self.logQueue = logQueue
        self.stopQueue = stopQueue
        self.finishSettingIDQueue = finishSettingIDQueue
        self.lastTime = arrow.now().datetime

        self.lastSymbol = ''
        self.datas = []

        self.active = False

    def initDB(self):
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

        # 回测任务参数队列
        self.argCol = self.db[self.config.get('mongo', 'argCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        self.resultCol = self.db[self.config.get('mongo', 'resultCol')].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

    def _shutdown(self, signalnum, frame):
        self.stop()

    def stop(self):
        self.active = False

    def start(self):
        self.initDB()
        self.active = True
        super(Optimization, self).start()

    def run(self):
        while self.active:
            try:
                stop = self.stopQueue.get_nowait()
                if stop == self.SIG_STOP:
                    self.active = False
                    break
            except Empty:
                pass
            try:
                self._run()
            except Empty:
                continue

    def _run(self):
        setting = self.settingQueue.get(timeout=1)

        _id = setting.pop('_id')
        vtSymbol = setting['vtSymbol']
        # self.log('info', str(setting))
        # 执行回测
        engine = runBacktesting(vtSymbol, setting, setting['className'], isShowFig=False, isOutputResult=False)

        if self.lastSymbol == vtSymbol and arrow.now().datetime - self.lastTime < datetime.timedelta(minutes=1):
            # 设置成历史数据已经加载
            engine.datas = self.datas
            engine.loadHised = True

        engine.runBacktesting()  # 运行回测

        self.lastSymbol = vtSymbol
        self.datas = engine.datas
        self.lastTime = arrow.now().datetime

        # 输出回测结果
        try:
            engine.showDailyResult()
            engine.showBacktestingResult()
        except:
            print(vtSymbol, setting['optsv'])
            self.log('error', traceback.format_exc())
            raise

        # 更新回测结果
        # 逐日汇总
        setting.update(engine.dailyResult)
        # 逐笔汇总
        setting.update(engine.tradeResult)

        # 将回测结果进行保存
        setting['datetime'] = arrow.now().datetime

        # 将 datetime.date 转化为 datetime.datetime
        for k, v in list(setting.items()):
            if isinstance(v, datetime.date):
                v = datetime.datetime.combine(v, datetime.time())
                v = self.localzone.localize(v)
                setting[k] = v

        # # 是否有至少一笔成交
        # if engine.tradeResult:
        #     # 有成交才保存这个，否则不保存回测结果
        #     self.resultCol.insert_one(setting)
        # # 无论是否有成交结果，删除掉任务
        # self.finishSettingIDQueue.put(_id)
        # self.argCol.delete_one({'_id': _id})

    def log(self, level, msg):
        """

        :param level:
        :param msg:
        :return:
        """
        self.logQueue.put((self.name, level, msg))


if __name__ == '__main__':
    server = OptimizeService()
    server.start()
