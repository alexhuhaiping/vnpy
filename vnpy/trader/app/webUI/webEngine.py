# encoding: UTF-8

from flask import Flask
from threading import Thread
import json
import pandas as pd

app = Flask(__name__)

if __debug__:
    from vnpy.trader.svtEngine import MainEngine

class WebEngine(object):
    def __init__(self, mainEngine, eventEngine):
        self.app = app
        self.app.mainEngine = mainEngine

        assert isinstance(app.mainEngine, MainEngine)

        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        self.thread = Thread(target=self._run)
        self.thread.start()

    def stop(self):
        del self.thread

    def _run(self):
        debug = False
        # if __debug__:
        #     debug = True
        app.run(debug=debug, host='0.0.0.0', port=8080)


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
    strategyDetails = []
    html = ''
    for ctaName, ctaStrategy in ctaApp.strategyDict.items():
        html += ctaName
        html += '</br>'
        html += ctaStrategy.className
        html += '</br>'
        html += pd.DataFrame([ctaStrategy.paramList2Html()], index=['param']).to_html()
        html += pd.DataFrame([ctaStrategy.varList2Html()], index=['var']).to_html()
        html += '</br>'
        html += '</br>'
    return html




if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
