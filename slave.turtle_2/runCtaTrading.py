# encoding: UTF-8

import signal
from time import sleep
import logging
import traceback
import logging.config

from vnpy.event import EventEngine2
from vnpy.trader.svtEngine import MainEngine
from vnpy.trader.gateway import ctpGateway
from vnpy.trader.app import ctaStrategy
from vnpy.trader.app import webUI
from vnpy.trader.app import riskManager
from vnpy.trader.vtFunction import exception, getJsonPath


def main():
    """主函数"""

    # 读取日志配置文件
    loggingConFile = 'logging.conf'
    loggingConFile = getJsonPath(loggingConFile, __file__)
    logging.config.fileConfig(loggingConFile)


    ee = EventEngine2()
    ee.log.info(u'===================')
    ee.log.info(u'事件引擎创建成功')

    me = MainEngine(ee)
    me.log.info(u'主引擎创建成功')

    @exception
    def shutdownFunction(signalnum, frame):
        me.log.info(u'系统即将关闭')
        me.exit()

    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, shutdownFunction)
        signal.siginterrupt(sig, False)

    # 执行连接到数据库
    # 大部分的功能依赖于 db 接口
    me.dbConnect()

    me.addGateway(ctpGateway)
    me.addApp(ctaStrategy)
    me.log.info(u'启动网页UI')
    me.addApp(webUI)  # 网页UI
    me.addApp(riskManager) # 风控模块

    me.connect('CTP')
    me.log.info(u'连接CTP接口')

    sleep(5)  # 等待CTP接口初始化

    cta = me.appDict[ctaStrategy.appName]

    cta.loadSetting()
    cta.log.info(u'CTA策略载入成功')

    cta.initAll()
    cta.log.info(u'CTA策略初始化成功')

    cta.startAll()
    cta.log.info(u'CTA策略启动成功')

    me._active = True
    me.run_forever()


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger = logging.getLogger()
        err = traceback.format_exc()
        print(err)
        logger.critical(err)
        sleep(1)

