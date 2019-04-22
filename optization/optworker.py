import os
import logging
import threading
import time
from queue import Empty
import hashlib
import pytz
from bson.codec_options import CodecOptions
import configparser
import multiprocessing
import signal
import datetime
import traceback
import requests
from io import FileIO, BytesIO

try:
    import pickle as pickle
except ImportError:
    import pickle

from slavem import Reporter
import arrow
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING

from vnpy.trader.vtFunction import getTempPath, getJsonPath
import optcomment
import optchild


class Optimization(object):
    """
    执行优化的子进程
    """
    SIG_STOP = 'close_service'

    def __init__(self, name, stoped, logQueue, config=None, *args, **kwargs):
        self.localzone = pytz.timezone('Asia/Shanghai')
        self.logQueue = logQueue
        self.name = name

        self.config = configparser.SafeConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        self.log = logging.getLogger('{}_{}'.format(self.config.get('slavem', 'localhost'), self.name))

        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        self.gitHash = r.read().strip('\n')

        self.salt = self.config.get('web', 'salt').encode()
        self.interval = 1
        self.lastTime = arrow.now().datetime

        self.lastSymbol = ''

        self.stoped = stoped

        self.childRunMaxTime = self.config.getint('worker', 'childRunMaxTime')
        self.childRunCount = 0
        self.tasks = multiprocessing.Queue()
        self.results = multiprocessing.Queue()
        self.childStoped = multiprocessing.Event()
        self.child = None  # 运行回测子进程实例

    def shutdown(self, signalnum, frame):
        self.stop()

    # def log(self, level, text):
    #     self.logQueue.put((self.name, level, text))

    def stop(self):
        if not self.stoped.wait(0):
            self.stoped.set()
        if not self.childStoped.wait(0):
            self.childStoped.set()

    def start(self):
        self.log.info('启动 {}'.format(self.name))
        self.run()

    def run(self):
        while not self.stoped.wait(self.interval):
            try:
                self._run()
            except Exception:
                self.log.critical(traceback.format_exc())
                self.log.critical('异常退出')
                break
        self.log.info('{} 退出'.format(self.name))
        self.stop()

    def _run(self):
        # 长时间闲置，关闭子进程
        if self.child is not None and time.time() - self.lastTime > 60 * 10:
            # 闲置超过10分钟
            self.log.info('算力闲置，关闭子进程')
            self.dropChild()

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
            self.log.critical(traceback.format_exc())
            return

        if r.text == '版本不符':
            self.log.error('版本不符')
            self.setLongWait()
            return
        if r.text == '没有任务':
            self.log.info('没有回测任务')
            self.setLongWait()
            return
        if r.text == '':
            self.log.error('没有提供版本号')
            self.setLongWait()
            return

        data = pickle.loads(r.content)

        setting = data['setting']
        if setting is None:
            # 没有得到任务，放弃
            self.log.debug('optserver 没有任务')
            self.setLongWait()
            return

        self.log.info('开始运行回测 {vtSymbol} {optsv}'.format(**setting))

        # 在子进程中运行回测
        result = self.dobacktesting(setting)
        if result is None:
            return

        # 重置闲置时间
        self.lastTime = time.time()

        self.log.info('回测结束')
        sendInterval = 0
        while not self.stoped.wait(sendInterval):
            try:
                self.sendResult(result)
                break
            except Exception:
                sendInterval = 10
                self.log.error(traceback.format_exc())

    def dropChild(self):
        if self.child:
            if not self.childStoped.wait(0):
                self.childStoped.set()
            self.child.join(2)
            self.child.terminate()
            self.child = None

    def getSettingUrl(self):
        url = self.config.get('web', 'url')

        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        self.gitHash = r.read().strip('\n')
        return '{}/getsetting/{gitHash}'.format(url, gitHash=self.gitHash)

    def setLongWait(self):
        # 长时间待机
        self.log.info('长待机')
        self.interval = 60 * 5
        # self.interval = 0.1

    def setShortWait(self):
        self.interval = 0.01

    def getBtrUrl(self):
        """
        返回回测结果
        :return:
        """
        return self.config.get('web', 'url') + '/btr'

    def newChild(self):
        """

        :return:
        """
        # 重置子进程标记
        self.childStoped.clear()
        self.child = multiprocessing.Process(name=self.name, target=optchild.child,
                                             args=(
                                                 self.name, self.childStoped, self.tasks, self.results, self.logQueue))

        # self.child = threading.Thread(name=self.name, target=child,
        #                               args=(self.childStoped, self.tasks, self.results, self.logQueue))

        self.child.daemon = True
        # 子进程运行计数
        self.childRunCount = 0
        # 开始子进程
        self.child.start()
        self.log.info('使用新子进程')

    def dobacktesting(self, setting):
        vtSymbol = setting['vtSymbol']

        if self.childRunCount > self.childRunMaxTime:
            self.dropChild()

        if self.child and vtSymbol == self.lastSymbol:
            # 还存在可重复利用的子进程，不需要重新生成子进程
            self.log.info('重复利用子进程')
            self.childRunCount += 1
            self.log.info('子进程复用 {} 次'.format(self.childRunCount))
            pass
        else:  # 生成新的子进程
            self.dropChild()
            # 执行回测
            self.newChild()

        # 向子进程提交回测任务
        self.tasks.put(setting)

        # 等待回测结果出来
        sec = 0
        while not self.stoped.wait(0):
            try:
                sec += 1
                result = self.results.get(timeout=1)
                result = pickle.loads(result)
                # 获得了数据
                break
            except Empty:
                if sec > 60:
                    sec = 0
                    # 超过1分钟都没完成回测
                    self.log.info('回测 {vtSymbol} {optsv} 超过1分钟未完成'.format(**setting))
                    # 重新生成子进程
                    self.dropChild()
                    self.newChild()
                    while not self.tasks.empty():
                        self.tasks.get_nowait()
                    self.tasks.put(setting)
        else:
            # 服务正常关闭
            self.log.info('算力线程退出')
            return

        self.lastSymbol = vtSymbol
        self.lastTime = arrow.now().datetime

        # 将回测结果进行保存
        result['datetime'] = arrow.now().datetime

        # 将 datetime.date 转化为 datetime.datetime
        for k, v in list(result.items()):
            if isinstance(v, datetime.date):
                v = datetime.datetime.combine(v, datetime.time())
                v = self.localzone.localize(v)
                result[k] = v

        return result

    def sendResult(self, result):
        # 返回回测结果
        data = {'result': result}
        dataPickle = pickle.dumps(data)
        _hash = optcomment.saltedByHash(dataPickle, self.salt)
        data = {'data': dataPickle, 'hash': _hash}

        dataPikcle = BytesIO(dataPickle)
        url = self.getBtrUrl()
        r = requests.post(url, data=data, files={'data': dataPikcle})
        dataPikcle.close()

        if r.status_code != 200:
            msg = f'返回码异常 {r.status_code} != 200'
            msg += f'\n {data}'
            raise ValueError(msg)

        # 正常完成回测，继续下一个
        self.setShortWait()



def childProcess(name, stoped, logQueue, config=None):
    woker = Optimization(name, stoped, logQueue, config)
    woker.start()


if __name__ == '__main__':
    # w = Optimization()
    # w.start()
    import queue

    stoped = multiprocessing.Event()
    logQueue = multiprocessing.Queue()
    w = Optimization('work_test', stoped, logQueue)

    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, w.shutdown)


    def log():
        while not stoped.wait(0):
            try:
                level, text = logQueue.get(timeout=1)
                func = getattr(w.log, level)
                func(text)
            except Empty:
                pass


    threading.Thread(target=log).start()
    w.start()
