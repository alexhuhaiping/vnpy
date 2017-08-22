# encoding: UTF-8

from flask import Flask
from threading import Thread
import json
import pandas as pd

app = Flask(__name__)

if __debug__:
    try:
        from vnpy.trader.vtEngine import MainEngine

        assert isinstance(app.mainEngine, MainEngine)
    except AssertionError:
        pass

class WebEngine(object):
    def __init__(self, mainEngine, eventEngine):
        self.app = app
        self.app.mainEngine = mainEngine
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        self.thread = Thread(target=self._run)
        self.thread.start()

    def stop(self):
        del self.thread

    def _run(self):
        debug = False
        if __debug__:
            debug = True
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
        html += pd.DataFrame([ctaStrategy.__dict__]).to_html()
    return html
    # df = pd.DataFrame(strategyDetails )
    # return df.to_html()



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
