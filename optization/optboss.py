# coding:utf-8
import logging
import multiprocessing
import signal
import threading
import traceback
import ConfigParser
from Queue import Empty

from vnpy.trader.vtFunction import getTempPath, getJsonPath
from optworker import childProcess

class WorkService(object):
    """

    """

    def __init__(self, config=None):

        # 要使用的CPU数量
        self.cpuCount = multiprocessing.cpu_count()
        if __debug__:
            self.cpuCount = min(2, self.cpuCount)

        self.config = ConfigParser.SafeConfigParser()
        configPath = config or getJsonPath('optimize.ini', __file__)
        with open(configPath, 'r') as f:
            self.config.readfp(f)

        self.log = logging.getLogger('{}_boss'.format(self.config.get('slavem', 'localhost')))

        self.logs = {}
        self.workers = []

        self.logQueue = multiprocessing.Queue()
        self.stoped = multiprocessing.Event()

        # 以子线程来运行 optwork
        for i in range(self.cpuCount):
            name = 'wodker_{}'.format(i+1)
            w = threading.Thread(name=name, target=childProcess, args=(name, self.stoped, self.logQueue, config))
            self.workers.append(w)

        for w in self.workers:
            self.logs[w.name] = logging.getLogger(w.name)

        self.logging = True
        self.log.info(u'即将启动 {} 个svnpy优化算力'.format(self.cpuCount))

        # 输出日志
        # self.logForever = threading.Thread(name='log', target=self._log)

        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self.shutdown)
            signal.siginterrupt(sig, False)

    def start(self):
        self.log.warning(u'分布式回测算力加入'.format(self.config.get('slavem', 'localhost')))
        # self.logForever.start()
        for w in self.workers:
            w.start()
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
        self.stoped.wait(3)
        while self.logging:
            try:
                name, level, text = self.logQueue.get(timeout=1)
                log = self.logs[name]
                func = getattr(log, level)
                func(text)
            except Empty:
                continue
            except Exception:
                self.log.critical(traceback.format_exc())
                raise

        self.log.info(u'完全退出')

if __name__ == '__main__':
    # import logging.config
    # 读取日志配置文件
    # loggingConFile = 'logging.conf'
    # loggingConFile = getJsonPath(loggingConFile, __file__)
    # logging.config.fileConfig(loggingConFile)
    # print(1414141)

    optfile = 'optimize.ini'
    # optfile = 'optimizeHome.ini'
    server = WorkService(optfile)
    server.start()
