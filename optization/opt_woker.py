import os
import configparser
from threading import Event
import pickle
import traceback

import redis

import runBacktesting
import redis_queue


class OptWoker(object):
    """

    """

    def __init__(self, name, confg):
        self.name = name
        self.config = configparser.ConfigParser()
        with open(confg, 'r') as f:
            self.config.read_file(f)

        self.stoped = Event()
        self.woker_conf = self.config['worker']
        self.max_run = self.woker_conf.getint('childRunMaxTime')

        # 链接 redis 消息队列
        self.redis_confg = self.config['redis']
        redis_kwarg = dict(
            host=self.redis_confg['host'],
            port=self.redis_confg.getint('port'),
            password=self.redis_confg['password']
        )

        self.redis = redis.Redis(decode_responses=True, **redis_kwarg)

        # 日志队列
        self.log = Log(name, redis_queue.RedisQueue(self.redis_confg['worklog'], **redis_kwarg))

        # 任务队列
        self.task_queue = redis_queue.RedisQueue(self.redis_confg['btarg'], **redis_kwarg)
        # 回测结果队列
        self.result_queue = redis_queue.RedisQueue(self.redis_confg['btresult'], **redis_kwarg)

        self.engine = None  # backtestEngine

        self.bars = []  # 指定的 vtSymbol 的行情缓存
        self.vtSymbol = ''  # 当前 self.bars 的合约

    def pack(self, data):
        return pickle.dumps(data)

    def unpack(self, data):
        return pickle.loads(data)

    @classmethod
    def child(cls, name, config):
        woker = cls(name, config)
        woker.start()

    def start(self):
        try:
            self.run()
        except KeyboardInterrupt:
            self.stoped.set()

        self.stop()

    def stop(self):
        self.log.info('即将退出')

    def run(self):
        while not self.stoped.wait(1):
            # 对比 git hash
            server_git_hash = self.redis.get('git')
            local_git_hash = self.get_git_hash()
            if server_git_hash != local_git_hash:
                self.log.warning(f'git hash 不符 local {local_git_hash} server {server_git_hash}')
                self.stoped.set()
                break

            # 获取回测参数
            setting = self.task_queue.get_nowait()
            if setting is not None:
                result = self.newEngine(self.unpack(setting))

                # 将回测结果放回队列
                self.result_queue.put(self.pack(result))

    def get_git_hash(self):
        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        git_hash = r.read().strip('\n')
        return git_hash

    def newEngine(self, setting):
        vtSymbol = setting['vtSymbol']
        engine = runBacktesting.runBacktesting(vtSymbol, setting, setting['className'], isShowFig=False,
                                               isOutputResult=False)

        if vtSymbol == self.vtSymbol and self.bars:
            # 设置成历史数据已经加载
            engine.datas = self.bars
            engine.loadHised = True
        else:
            # engine 需要自己重新加载合约
            self.bars.clear()

        self.log.info('开始运行回测')
        engine.runBacktesting()  # 运行回测
        self.log.info('回测运行完毕')

        # 缓存合约行情
        self.bars = engine.datas
        self.vtSymbol = vtSymbol

        # 输出回测结果
        try:
            engine.showDailyResult()
            engine.showBacktestingResult()
        except IndexError:
            pass
        except Exception:
            self.log.error('{} {}'.format(vtSymbol, setting['optsv']))
            self.log.error(traceback.format_exc())
            raise

        # 逐日汇总
        setting.update(engine.dailyResult)
        # 逐笔汇总
        setting.update(engine.tradeResult)

        engine.closeMongoDB()
        # 销毁实例，尝试回收内存
        del engine

        return setting


class Log():
    def __init__(self, name, log_queue):
        self.name = name
        self.log_queue = log_queue

    def pack(self, name, level, msg):
        return pickle.dumps((self.name, level, msg))

    def debug(self, msg):
        self.log_queue.put(self.pack(self.name, 'debug', msg))

    def info(self, msg):
        self.log_queue.put(self.pack(self.name, 'info', msg))

    def warning(self, msg):
        self.log_queue.put(self.pack(self.name, 'warning', msg))

    def error(self, msg):
        self.log_queue.put(self.pack(self.name, 'error', msg))

    def critical(self, msg):
        self.log_queue.put(self.pack(self.name, 'critical', msg))
