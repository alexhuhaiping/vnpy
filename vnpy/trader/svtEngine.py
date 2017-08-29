# encoding: UTF-8

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

from vnpy.trader.language import text
from vnpy.trader.vtGateway import *
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtEngine import MainEngine as VtMaingEngine


########################################################################
class MainEngine(VtMaingEngine):
    """主引擎"""

    # ----------------------------------------------------------------------
    def __init__(self, eventEngine):
        super(MainEngine, self).__init__(eventEngine)
        self.ctpCollection = None  # 历史行情数据库
        self.ctaDB = None  # cta 策略相关的数据

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
                self.ctpCollection = ctpdb['bar_1min']

                ctadb = self.dbClient[globalSetting['mongoCtaDbn']]
                ctadb.authenticate(globalSetting['mongoCtaUsername'], globalSetting['mongoCtaPassword'])
                self.ctaDB = ctadb

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                self.writeLog(text.DATABASE_CONNECTING_COMPLETED)

                # 如果启动日志记录，则注册日志事件监听函数
                if globalSetting['mongoLogging']:
                    self.eventEngine.register(EVENT_LOG, self.dbLogging)

            except ConnectionFailure:
                self.writeLog(text.DATABASE_CONNECTING_FAILED)

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
