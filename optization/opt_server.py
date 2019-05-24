import logging.config
import os
import datetime
import pickle
import configparser
from bson import CodecOptions
import pytz
from threading import Event
import time

import arrow
import pymongo
import redis

import redis_queue

LOCA_AWARE = pytz.timezone('Asia/Shanghai')


class OptimizeService(object):
    """
    对比已经完成的回测和参数
    生成任务队列
    获取结果队列
    保存结果
    """
    MAX_TASK = 100  # 队列最多保持几个任务
    MAX_RESULT = 100  # 任务最多保持接

    def __init__(self, confg):
        loggingConFile = 'logging.conf'
        logging.config.fileConfig(loggingConFile)
        self.stoped = Event()

        self.log = logging.getLogger()

        self.config = configparser.ConfigParser()
        with open(confg, 'r') as f:
            self.config.read_file(f)

        # 链接回测数据库
        mongo_confg = self.config['backtesting_mongo']
        self.mongo_client = pymongo.MongoClient(
            host=mongo_confg['host'],
            port=mongo_confg.getint('port'),
        )

        self.db = self.mongo_client[mongo_confg['dbn']]
        self.db.authenticate(
            mongo_confg['username'],
            mongo_confg['password']
        )

        # 回测任务参数
        self.arg_col = self.db[mongo_confg['btarg']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCA_AWARE))

        # 回测结果存库
        self.result_col = self.db[mongo_confg['btresult']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

        # 链接 redis 消息队列
        self.redis_confg = self.config['redis']
        redis_kwarg = dict(
            host=self.redis_confg['host'],
            port=self.redis_confg.getint('port'),
            password=self.redis_confg['password']
        )

        # 用于统计保存结果的速度
        self.last_result_time = self.begin = arrow.now().datetime
        self.save_count = 0
        self.total = 0  # 任务总数

        # 任务队列
        self.redis = redis.Redis(decode_responses=True, **redis_kwarg)
        self.task_queue = redis_queue.RedisQueue(self.redis_confg['btarg'], **redis_kwarg)

        # 回测结果队列
        self.result_queue = redis_queue.RedisQueue(self.redis_confg['btresult'], **redis_kwarg)

        # 不会自动结束
        self.auto_close = False
        self.free_2_auto_close_time = datetime.timedelta(seconds=30)

    def set_auto_close(self, flag=True, timeout=30):
        self.auto_close = flag
        self.free_2_auto_close_time = datetime.timedelta(seconds=timeout)

    def check(self):
        """
        检查并剔除已经完成的任务
        :return:
        """
        self.log.info('开始核对已经完成的回测任务')
        dic = self.result_col.find_one({}, {}, no_cursor_timeout=True)
        if not dic:
            self.log.info('不需要核对回测任务')
            return

        dic = self.arg_col.find_one({}, {}, no_cursor_timeout=True)
        if not dic:
            self.log.info('没有需要核对的回测任务')
            return

        cursor = self.arg_col.find({}, {'optsv': 1, 'vtSymbol': 1}, no_cursor_timeout=True)
        argsIDs = {d['optsv'] + '@@' + d['vtSymbol'] for d in cursor}

        cursor = self.result_col.find({}, {'optsv': 1, 'vtSymbol': 1}, no_cursor_timeout=True)
        resultIDs = {d['optsv'] + '@@' + d['vtSymbol'] for d in cursor}

        # 对比回测
        finishIDs = argsIDs & resultIDs

        self.log.info('已经完成任务 {} of {}'.format(len(finishIDs), len(argsIDs)))

        self.total = len(argsIDs) - len(finishIDs)

        # 删除已经完成的回测参数
        for _id in finishIDs:
            optsv, vtSymbol = _id.split('@@')
            self.arg_col.delete_one({'optsv': optsv, 'vtSymbol': vtSymbol})

    def set_git_hash(self):
        cmd = "git log -n 1 | head -n 1 | sed -e 's/^commit //' | head "
        r = os.popen(cmd)
        git_hash = r.read().strip('\n')
        self.log.info(f'server git hash {git_hash}')
        self.redis.set('git', git_hash)

    def start(self):
        self.log.info('清除 task 和 result 队列')
        # 清空任务队列
        self.task_queue.clear()

        # 清空结果队列
        self.result_queue.clear()

        self.set_git_hash()

        # 对比已经完成的任务
        self.check()

        self.last_result_time = self.begin = arrow.now().datetime

    def run(self):

        try:
            while not self.stoped.wait(0):
                self._run()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.stoped.set()
        self.log.info('即将退出服务')
        self.task_queue.clear()
        # 清空结果队列
        self.result_queue.clear()
        self.log.info('完全退出服务')

    def save_result(self):
        results = []
        while not self.stoped.wait(0):
            r = self.result_queue.get_nowait()
            if r is None:
                break
            else:
                r = self.unpack(r)
                results.append(r)

        # 批量保存回测结果
        if results:
            self.save_count += len(results)
            self.result_col.insert_many(results)
            self.last_result_time = arrow.now().datetime
            per = round(self.save_count / self.total * 100, 1)
            cost = self.last_result_time - self.begin
            avr = round(int(cost.total_seconds()) / self.save_count, 2)
            need = cost / (self.save_count / self.total) - cost
            self.log.info(f'已完成 {self.save_count} / {self.total} {per}% 均速 {avr} 仍需 {need}')

        return results

    def _run(self):
        self.log.info('进入主循环')
        cursor = self.arg_col.find({})
        while not self.stoped.wait(1):
            # 检查任务数量，并补充任务
            supp = self.MAX_TASK - self.task_queue.qsize()
            for i in range(max(1, supp)):
                try:
                    setting = next(cursor)
                    self.task_queue.put(
                        self.pack(setting)
                    )

                except StopIteration:
                    self.log.info('任务已经全部分配')
                    break

            # 检查回测结果数量，存储回测结果
            results = self.save_result()
            # 对是否自动关闭服务进行判定
            if not results and self.auto_close:
                if arrow.now().datetime - self.last_result_time > self.free_2_auto_close_time:
                    self.check()  # 核对是否已经全部完成任务,清除掉mongodb btarg collection中已经完成的任务
                    if self.arg_col.find_one({}):
                        # 清除掉已完成的，依然还有任务，通过 return 重置 self._run()
                        self.log.info('新一轮回测')
                        self.last_result_time = arrow.now().datetime
                        return
                    else:
                        self.log.info('所有批量回测完成，自动关闭 server ')
                        self.stop()

    def pack(self, data):
        return pickle.dumps(data)

    def unpack(self, data):
        return pickle.loads(data)


if __name__ == '__main__':
    optfile = 'optimize.ini'
    opts = OptimizeService(optfile)
    opts.start()
