# coding:utf-8

import os
from Queue import Empty, Full
import threading
import json
import pytz
from bson.codec_options import CodecOptions
import ConfigParser
import time
import logging.config
import multiprocessing
import signal
import datetime
import traceback
import requests

try:
    import cPickle as pickle
except ImportError:
    import pickle

from slavem import Reporter
import arrow
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING

from vnpy.trader.vtFunction import getTempPath, getJsonPath
from runBacktesting import runBacktesting
import optcomment


class Optimization(object):
    """
    执行优化的子进程
    """
    SIG_STOP = 'close_service'

    def __init__(self, name, stoped, logQueue, config=None, *args, **kwargs):
        self.localzone = pytz.timezone('Asia/Shanghai')
        self.logQueue = logQueue
        self.name = name

        self.config = ConfigParser.SafeConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        self.gitHash = r.read().strip('\n')

        self.salt = self.config.get('web', 'salt')
        self.interval = 1
        self.lastTime = arrow.now().datetime

        self.lastSymbol = ''
        self.datas = []

        self.stoped = stoped

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)

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

    def shutdown(self, signalnum, frame):
        if not self.stoped.wait(0):
            self.stoped.set()

    def log(self, level, text):
        self.logQueue.put((self.name, level, text))

    def stop(self):
        pass

    def start(self):
        self.log('info', u'启动 {}'.format(self.name))
        self.initDB()
        self.run()

    def run(self):
        while not self.stoped.wait(self.interval):
            try:
                self._run()
            except Exception:
                self.log('critical', traceback.format_exc())
                self.log('critical', u'异常退出')
                break
        self.log('info', u'子进程退出')
        self.stop()

    def _run(self):
        # 尝试获取任务
        try:
            url = self.getSettingUrl()
            r = requests.get(url)
            if r.status_code != 200:
                # 服务端可能没有开启,等待60秒
                self.setLongWait()
                return
        except requests.ConnectionError:
            self.setLongWait()
            return
        except Exception:
            self.setLongWait()
            self.log('critical', traceback.format_exc())
            return

        data = pickle.loads(r.text.encode('utf-8'))

        setting = data['setting']
        if setting is None:
            # 没有得到任务，放弃
            self.setLongWait()
            return

        result = self.dobacktesting(setting)
        self.log('info', u'回测结束')

        self.sendResult(result)

    def getSettingUrl(self):
        url = self.config.get('web', 'url')

        return '{}/getsetting/{gitHash}'.format(url, gitHash=self.gitHash)

    def setLongWait(self):
        # 长时间待机
        self.interval = 60

    def setShortWait(self):
        self.interval = 0.01

    def getBtrUrl(self):
        """
        返回回测结果
        :return:
        """
        return self.config.get('web', 'url') + '/btr'

    def dobacktesting(self, setting):
        setting = setting.copy()
        vtSymbol = setting['vtSymbol']

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
            self.log('error', u'{} {}'.format(vtSymbol, setting['optsv']))
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

        return setting

    def sendResult(self, result):
        # 返回回测结果
        data = {'result': result}
        dataPickle = pickle.dumps(data)
        data = {'data': dataPickle, 'hash': optcomment.saltedByHash(dataPickle, self.salt)}

        url = self.getBtrUrl()
        r = requests.post(url, data=data)
        if r.status_code != 200:
            self.setLongWait()
            return

        # 正常完成回测，继续下一个
        self.setShortWait()


def childProcess(name, stoped, logQueue, config=None):
    woker = Optimization(name, stoped, logQueue, config)
    woker.start()


if __name__ == '__main__':
    # w = Optimization()
    # w.start()
    import Queue

    stoped = threading.Event()
    logQueue = Queue.Queue()
    w = Optimization('work_test', stoped, logQueue)
    w.start()
    while not w.stoped.wait(1):
        pass