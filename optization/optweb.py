# encoding: utf-8
import time
import logging
from werkzeug.serving import make_server
import traceback
from flask import Flask, request
from threading import Thread

try:
    import Queue as queue
except ImportError:
    import queue

try:
    import cPickle as pickle
except ImportError:
    import pickle

import optcomment

app = Flask(__name__)
PORT = 30050


@app.route('/getsetting/<gitHash>/')
def requestSetting(gitHash):
    """
    算力请求任务
    :param data:
    :return:
    """
    logger = logging.getLogger()
    try:
        if not gitHash:
            logger.debug(u'没有提供版本号')
            return u''

        if gitHash != app.optServer.gitHash:
            logger.debug(u'版本不符')
            return u'版本不符'

        # 校验通过，尝试返回需要回测的参数
        try:
            setting = app.optServer.tasksQueue.get(timeout=1)
        except queue.Empty:
            return u'没有任务'

        logger.info(u'{vtSymbol} {optsv}'.format(**setting))
        data = {
            'setting': setting,
        }

        data = pickle.dumps(data)
        return data

    except Exception:
        logger = logging.getLogger()
        err = traceback.format_exc()
        logger.warning(err)
        return ''


@app.route('/btr', methods=['POST'])
def btr():
    logger = logging.getLogger()
    originHash = request.form['hash']
    dataPickle = request.form['data']

    localHash = optcomment.saltedByHash(dataPickle, app.salt)

    if str(localHash) != originHash:
        logger.warning(u'hash不符合')
        return ''

    result = pickle.loads(dataPickle.encode('utf-8'))['result']

    app.optServer.accpetResult(result)
    return ''


class ServerThread(Thread):
    def __init__(self, optServer):
        Thread.__init__(self)
        self.optServer = optServer
        app.salt = optServer.salt
        app.optServer = optServer

        self.daemon = True
        self.name = u'web'
        self.setDaemon(True)
        self.srv = make_server('0.0.0.0', PORT, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


if __name__ == '__main__':
    n = 0
    s = ServerThread(app)
    s.start()
    while True:
        n += 1
        time.sleep(1)
        if n > 10:
            break
