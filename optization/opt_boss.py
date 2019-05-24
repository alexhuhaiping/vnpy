import logging.config
import configparser
from threading import Event
import multiprocessing
import pickle

import redis_queue
import opt_woker


class OptBoss(object):
    """

    """

    def __init__(self, confg):
        self.confg_file = confg
        loggingConFile = 'logging.conf'
        logging.config.fileConfig(loggingConFile)
        self.log = logging.getLogger()
        self.log_wokers = {}

        self.config = configparser.ConfigParser()
        with open(confg, 'r') as f:
            self.config.read_file(f)

        self.stoped = Event()

        # 链接 redis 消息队列
        self.redis_confg = self.config['redis']
        self.woker_confg = self.config['worker']

        redis_kwarg = dict(
            host=self.redis_confg['host'],
            port=self.redis_confg.getint('port'),
            password=self.redis_confg['password']
        )

        # 日志队列
        self.log_queue = redis_queue.RedisQueue(self.redis_confg['worklog'], **redis_kwarg)
        self.wokders = []

    def start(self):
        """

        :return:
        """
        # 清除日志队列
        self.log_queue.clear()

        # 生成子进程
        for i in range(self.woker_confg.getint('cpu')):
            name = f'workder_{i + 1}'
            self.log.info(f'生成子进程 {name}')
            child = multiprocessing.Process(name=name, target=opt_woker.OptWoker.child,
                                            args=(name, self.confg_file))
            self.wokders.append(child)
            log = self.log_wokers[name] = logging.getLogger(name)
            log.parent = logging.getLogger('woker')
            child.daemon = True
            child.start()

    def run(self):
        try:
            self._run()
        except KeyboardInterrupt:
            self.stoped.set()

        self.stop()

    def stop(self):
        self.stoped.set()
        self.log.info('准备退出服务')
        self.log_queue.clear()
        self.log.info('退出服务完成')

    def _run(self):
        """

        :return:
        """
        while not self.stoped.wait(0):
            log = self.log_queue.get_wait(1)
            if log:
                k, log = log
                name, level, msg = pickle.loads(log)
                logger = self.log_wokers[name]
                getattr(logger, level)(msg)



if __name__ == '__main__':
    optfile = 'optimize.ini'
    optb = OptBoss(optfile)
    optb.start()
