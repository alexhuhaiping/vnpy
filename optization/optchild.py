# coding:utf-8
import traceback
from runBacktesting import runBacktesting

try:
    import Queue as queue
except ImportError:
    import queue
import pickle
import signal


def newEngine(datas, setting, results, log):
    vtSymbol = setting['vtSymbol']
    engine = runBacktesting(vtSymbol, setting, setting['className'], isShowFig=False,
                            isOutputResult=False)

    if datas:
        # 设置成历史数据已经加载
        engine.datas = datas[0]
        engine.loadHised = True
    log('info', u'开始运行回测')
    engine.runBacktesting()  # 运行回测
    log('info', u'回测运行完毕')
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

    engine.closeMongoDB()
    # 销毁实例，尝试回收内存
    del engine


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
            setting = tasks.get(timeout=0.1)
        except queue.Empty:
            continue
        try:
            newEngine(datas, setting, results, log)
        except Exception:
            log('error', u'子进程异常退出')
            log('error', traceback.format_exc())
            stoped.set()
            return  # 异常退出

    # 正常退出子进程
    log('info', u'子进程正常退出')
