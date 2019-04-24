# encoding: UTF-8
import logging
from werkzeug.serving import make_server
import traceback
from flask import Flask
from threading import Thread
import pandas as pd

from vnpy.trader.vtGlobal import globalSetting

app = Flask(__name__)

if __debug__:
    from vnpy.trader.svtEngine import MainEngine

PORT = 38080

class ServerThread(Thread):
    def __init__(self, app):
        Thread.__init__(self)
        self.setDaemon(True)
        try:
            PORT = globalSetting['webPORT']
        except Exception:
            PORT = 38080
            logging.warning('未配置web端口，使用默认端口38080')
        self.srv = make_server('0.0.0.0', PORT, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


class WebEngine(object):
    def __init__(self, mainEngine, eventEngine):
        self.app = app
        self.app.mainEngine = mainEngine
        self.log = logging.getLogger()

        assert isinstance(app.mainEngine, MainEngine)

        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        self.serverThread = ServerThread(app)
        self.serverThread.start()

    def stop(self):
        self.log.info('webEngine 即将关闭')
        self.serverThread.shutdown()


@app.route('/')
def index():
    return 'vnpy web UI'


@app.route('/strategy')
def showCtaStrategy():
    """

    :return:
    """
    ctaApp = app.mainEngine.appDict['CtaStrategy']
    if __debug__:
        from vnpy.trader.app.ctaStrategy import CtaEngine
        assert isinstance(ctaApp, CtaEngine)

    dic = {'策略个数': 0, '总权益': 0}

    html = pd.DataFrame(ctaApp.accountToHtml()).to_html()
    html += '</br>'

    try:
        ctaApp.log.info('开始刷新 strategy 页面')
        strategyList = list(ctaApp.strategyDict.items())
        strategyList.sort(key=lambda s: s[0])
        for ctaName, ctaStrategy in strategyList:
            dic['策略个数'] += 1
            dic['总权益'] += int(ctaStrategy.rtBalance)

            html += ctaName
            html += '</br>'
            html += ctaStrategy.className
            html += '</br>'
            for index, data in list(ctaStrategy.toHtml().items()):
                if isinstance(data, dict):
                    html += pd.DataFrame([data], index=[index]).to_html()
                else:
                    html += data

                html += '</br>'
            html += '</br>'
            html += '</br>'


        posDetailList = [posDetail.toHtml() for posDetail in ctaApp.mainEngine.dataEngine.detailDict.values() if posDetail.pos != 0]
        html = pd.DataFrame([dic]).to_html() + '</br>' + pd.DataFrame(posDetailList).sort_values('vtSymbol').to_html() + '</br>' + html
        ctaApp.log.info('获得 strategy 页面')

    except:
        err = traceback.format_exc() + '</br>' * 2
        html = err.replace('\n', '</br>') + html
        app.logger.error(html)
    return html


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
