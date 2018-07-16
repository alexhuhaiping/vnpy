# encoding: utf-8
import os
import time


def run_app(ppid, localGitHash, salt, logQueue, tasksQueue, resultQueue):
    if ppid == os.getpid():
        # 父进程中，不执行逻辑
        return
    import signal
    import logging
    # from werkzeug.serving import make_server
    import traceback
    from flask import Flask, request

    try:
        import Queue as queue
    except ImportError:
        import queue

    try:
        import cPickle as pickle
    except ImportError:
        import pickle

    import optcomment
    log = optcomment.Logger(logQueue)
    log.warning(u'启动web服务')
    app = Flask(__name__)
    PORT = 30050

    @app.route('/')
    def index():
        return 'Index'

    @app.route('/test/<data>')
    def test(data):
        return data

    @app.route('/beat/<data>')
    def beat(data):
        localHash = optcomment.saltedByHash('test', salt)
        if data == str(localHash):
            return u''

    @app.route('/getsetting/<gitHash>/')
    def requestSetting(gitHash):
        """
        算力请求任务
        :param data:
        :return:
        """
        try:
            if not gitHash:
                log.debug(u'没有提供版本号')
                return u''

            if gitHash != localGitHash:
                log.debug(u'版本不符')
                return u'版本不符'

            # 校验通过，尝试返回需要回测的参数
            try:
                setting = tasksQueue.get(timeout=1)
            except queue.Empty:
                return u'没有任务'

            log.info(u'{vtSymbol} {optsv}'.format(**setting))
            data = {
                'setting': setting,
            }

            data = pickle.dumps(data)
            return data

        except Exception:
            err = traceback.format_exc()
            log.warning(err)
            return ''

    @app.route('/btr', methods=['POST'])
    def btr():
        logger = logging.getLogger()
        originHash = request.form['hash']
        dataPickle = request.form['data']

        localHash = optcomment.saltedByHash(dataPickle, salt)

        if str(localHash) != originHash:
            logger.warning(u'hash不符合')
            return

        result = pickle.loads(dataPickle.encode('utf-8'))['result']

        try:
            resultQueue.put(result, timeout=5)
        except queue.Full:
            log.warning(u'缓存回测结果超时')
            return

        return ''

    # server = make_server('0.0.0.0', PORT, app)

    def shutdown(signalnum, frame):
        log.info(u'web关闭中……')
        server.stop(timeout=3)

    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, shutdown)

    from gevent.pywsgi import WSGIServer
    server = WSGIServer(('', PORT), app)
    server.serve_forever()

    log.info(u'web关闭')


if __name__ == '__main__':
    import multiprocessing
    import logging

    ppid = 0
    localGitHash = '1'
    salt = '2'
    logQueue = multiprocessing.Queue()
    tasksQueue = multiprocessing.Queue()
    resultQueue = multiprocessing.Queue()
    log = logging.getLogger()
    p = multiprocessing.Process(target=run_app, args=(ppid, localGitHash, salt, logQueue, tasksQueue, resultQueue))
    p.daemon = True
    p.start()
    while True:
        level, text = logQueue.get()
        func = getattr(log, level)
        func(text)
