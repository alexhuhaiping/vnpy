# coding:utf-8
import traceback
from runBacktesting import runBacktesting
try:
    import Queue as queue
except ImportError:
    import queue
import pickle
import signal


def child(name, stoped, tasks, results, logQueue):
    datas = []

    def shutdown(signalnum, frame):
        pass

    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, shutdown)

    def log(level, text):
        logQueue.put((name, level, text))

    while not stoped.wait(0):
        try:
            setting = tasks.get(timeout=1)
        # except queue.Empty:
        except Exception:
            continue
        vtSymbol = setting['vtSymbol']
        engine = runBacktesting(vtSymbol, setting, setting['className'], isShowFig=False,
                                isOutputResult=False)

        if datas:
            # 设置成历史数据已经加载
            engine.datas = datas[0]
            engine.loadHised = True
        engine.runBacktesting()  # 运行回测
        if not datas:
            datas.append(engine.datas)

        # 输出回测结果
        try:
            engine.showDailyResult()
            engine.showBacktestingResult()
        except IndexError:
            pass
        except Exception:
            log('error', u'{} {}'.format(vtSymbol, setting['optsv']))
            log('error', traceback.format_exc())
            raise

        # 逐日汇总
        setting.update(engine.dailyResult)
        # 逐笔汇总
        setting.update(engine.tradeResult)

        results.put(pickle.dumps(setting))

        # 销毁实例，尝试回收内存
        del engine

    # 正常退出子进程
    log('info', u'子进程正常退出')
