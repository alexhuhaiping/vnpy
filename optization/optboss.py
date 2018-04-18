# coding:utf-8
import logging
import logging.config
import multiprocessing
import signal
import threading
import traceback

from vnpy.trader.vtFunction import getTempPath, getJsonPath
from optworker import childProcess


class WorkService(object):
    """

    """

    def __init__(self):
        self.log = logging.getLogger('boss')

        # 要使用的CPU数量
        cpuCount = multiprocessing.cpu_count() - 1
        self.cpuCount = max(cpuCount, 1)
        if __debug__:
            self.cpuCount = min(2, self.cpuCount)

        self.logs = {}
        self.workers = []

        self.logQueue = multiprocessing.Queue()
        self.stoped = multiprocessing.Event()

        for i in range(self.cpuCount):
            name = 'wodker_{}'.format(i)
            w = multiprocessing.Process(name=name, target=childProcess, args=(name, self.stoped, self.logQueue))
            self.workers.append(w)

        self.logging = True
        self.log.info(u'即将启动 {} 个svnpy优化算力'.format(self.cpuCount))

        # 输出日志
        # self.logForever = threading.Thread(name='log', target=self._log)

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)

    def initLog(self):
        # 读取日志配置文件
        loggingConFile = 'logging.conf'
        loggingConFile = getJsonPath(loggingConFile, __file__)
        logging.config.fileConfig(loggingConFile)

        for w in self.workers:
            self.logs[w.name] = logging.getLogger(w.name)

    def start(self):
        # self.logForever.start()
        for w in self.workers:
            w.start()
        self.initLog()
        self.run()

    def shutdown(self, signalnum, frame):
        def stop():
            if not self.stoped.wait(1):
                self.stoped.set()

        threading.Timer(0, stop).start()

        for w in self.workers:
            self.log.info(u'等待 {} {} 结束'.format(w.name, id(w)))
            w.join(1)

        self.logging = False

    def run(self):
        """

        :return:
        """
        self.stoped.wait(3)
        while self.logging:
            try:
                name, level, text = self.logQueue.get(1)

                log = self.logs[name]
                func = getattr(log, level)
                func(text)
            except Exception:
                traceback.print_exc()


if __name__ == '__main__':
    server = WorkService()
    server.start()
