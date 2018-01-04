# encoding: UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import traceback
from threading import Thread
from bson.codec_options import CodecOptions
import pytz
from time import sleep

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from slavem import Reporter

from vnpy.trader.language import text
from vnpy.trader.vtGateway import *
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtEngine import MainEngine as VtMaingEngine
from vnpy.trader.vtFunction import LOCAL_TIMEZONE, exception

if __debug__:
    import vnpy.trader.debuginject as debuginject


########################################################################
class MainEngine(VtMaingEngine):
    """主引擎"""

    # ----------------------------------------------------------------------
    def __init__(self, eventEngine):
        super(MainEngine, self).__init__(eventEngine)
        self.ctpdb = None  # ctp 历史行情数据库
        # self.strategyDB = None  # cta 策略相关的数据

        if __debug__:
            self.log.warning(u'DEBUG 模式')

        self.active = False

        # 汇报
        self.slavemReport = Reporter(
            **globalSetting['slavem']
        )

    # ----------------------------------------------------------------------
    def dbConnect(self):
        """连接MongoDB数据库"""
        if not self.dbClient:
            # 读取MongoDB的设置
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                self.dbClient = MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'],
                                            connectTimeoutMS=500)
                ctpdb = self.dbClient[globalSetting['mongoCtpDbn']]
                ctpdb.authenticate(globalSetting['mongoUsername'], globalSetting['mongoPassword'])
                self.ctpdb = ctpdb

                ctaConfig = globalSetting['mongoCTA']
                globals().update(ctaConfig)
                client = MongoClient(
                    ctaConfig['host'],
                    ctaConfig['port']
                )
                db = client[ctaConfig['dbn']]
                db.authenticate(ctaConfig['username'], ctaConfig['password'])
                self.strategyDB = db

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                self.writeLog(text.DATABASE_CONNECTING_COMPLETED)

                # 如果启动日志记录，则注册日志事件监听函数
                if globalSetting['mongoLogging']:
                    self.eventEngine.register(EVENT_LOG, self.dbLogging)

            except ConnectionFailure:
                self.writeLog(text.DATABASE_CONNECTING_FAILED)
            except:
                self.log.critical(traceback.format_exc())
                raise

    # ----------------------------------------------------------------------
    def dbLogging(self, event):
        """向MongoDB中插入日志"""
        pass
        # log = event.dict_['data']
        # d = {
        #     'content': log.logContent,
        #     'time': log.logTime,
        #     'gateway': log.gatewayName
        # }
        # TODO 不保存数据到数据库
        # self.dbInsert(LOG_DB_NAME, self.todayDate, d)

    @staticmethod
    def createCollectionIndex(col, indexes):
        """
        初始化分钟线的 collection
        :return:
        """

        # 检查索引
        try:
            indexInformation = col.index_information()
            for indexModel in indexes:
                if indexModel.document['name'] not in indexInformation:
                    col.create_indexes(
                        [
                            indexModel,
                        ],
                    )
        except OperationFailure:
            # 有索引
            col.create_indexes(indexes)

    def qryMarginRate(self, gatewayName, vtSymbol):
        gateway = self.getGateway(gatewayName)
        if gateway:
            gateway.qryMarginRate(vtSymbol)

    def qryCommissionRate(self, gatewayName, vtSymbol):
        gateway = self.getGateway(gatewayName)
        if gateway:
            gateway.qryCommissionRate(vtSymbol)

    def testfunc(self):
        try:
            reload(debuginject)
            debuginject.me = self
            debuginject.run()
            sleep(2)
        except Exception as e:
            self.log.info(traceback.format_exc())


    @exception()
    def exit(self):
        super(MainEngine, self).exit()
        if __debug__:
            self._testActive = False

        self.active = False

    def run_forever(self):
        self.active = True

        self.log.info(u'开始运行')

        while self.active:
            if __debug__:
                self.testfunc()
            sleep(1)

        # 停止心跳
        self.log.info(u'停止心跳')
        self.slavemReport.endHeartBeat()

        self.log.info(u'系统完全关闭')
