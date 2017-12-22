# encoding: UTF-8

from werkzeug.serving import make_server
import traceback
from flask import Flask
from threading import Thread
import pandas as pd

app = Flask(__name__)

if __debug__:
    from vnpy.trader.svtEngine import MainEngine

PORT = 8080


class ServerThread(Thread):
    def __init__(self, app):
        Thread.__init__(self)
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

        assert isinstance(app.mainEngine, MainEngine)

        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        self.serverThread = ServerThread(app)
        self.serverThread.start()

    def stop(self):
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

    html = ''
    try:
        for ctaName, ctaStrategy in ctaApp.strategyDict.items():
            html += ctaName
            html += '</br>'
            html += ctaStrategy.className
            html += '</br>'
            for index, data in ctaStrategy.toHtml().items():
                if isinstance(data, dict):
                    html += pd.DataFrame([data], index=[index]).to_html()
                else:
                    html += data

                html += '</br>'
                # html += pd.DataFrame([ctaStrategy.paramList2Html()], index=['param']).to_html()
                # html += pd.DataFrame([ctaStrategy.varList2Html()], index=['var']).to_html()
            html += '</br>'
            html += '</br>'
    except:
        err = traceback.format_exc()
        html += '</br>'
        html += '</br>'
        html += err.replace('\n', '</br>')
        print(err)
    return html


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
