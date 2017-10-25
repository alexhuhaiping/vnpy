# encoding: UTF-8

'''
本文件中实现了行情数据记录引擎，用于汇总TICK数据，并生成K线插入数据库。

使用DR_setting.json来配置需要收集的合约，以及主力合约代码。
'''

import copy
import json
import os
import time
from datetime import datetime

from pymongo import IndexModel, ASCENDING, DESCENDING
from vtFunction import todayDate

from drBase import *
from eventEngine import *
from language import text
from vtGateway import VtSubscribeReq, VtLogData


########################################################################
class DrEngine(object):
    """数据记录引擎"""

    settingFileName = 'DR_setting.json'
    path = os.path.abspath(os.path.dirname(__file__))
    settingFileName = os.path.join(path, settingFileName)

    # ----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        # 当前日期
        self.today = todayDate()

        # 主力合约代码映射字典，key为具体的合约代码（如IF1604），value为主力合约代码（如IF0000）
        self.activeSymbolDict = {}

        # Tick对象字典
        self.tickDict = {}

        # K线对象字典
        self.barDict = {}

        # 负责执行数据库插入的单独线程相关
        self.active = False  # 工作状态
        # self.tickCache = {}  # 缓存队列 {'collcectionName': Queue()}
        # self.tickQueue = Queue()  # 队列
        self.queue = Queue()
        self.thread = Thread(target=self.run)  # 线程

        # 启动标志
        self._subcribeNum = 0
        self.startReport = False

        self.threadUpdateContractDetail = Thread(target=self.updateContractDetail)
        # 待更新保证金队列
        self.marginRateBySymbol = {}
        # 待更新的手续费率队列
        self.vtCommissionRateBySymbol = {}

        # 载入设置，订阅行情
        self.loadSetting()

        self.collectionNames = []

    def subscribeDrContract(self, event):
        """

        :param symbol:
        :return:
        """
        contract = event.dict_['data']
        if contract.productClass != u'期货':
            return
        vtSymbol = symbol = contract.symbol

        req = VtSubscribeReq()
        req.symbol = symbol

        # 记录 1min bar
        bar = DrBarData()
        self.barDict[vtSymbol] = bar

        self.mainEngine.subscribe(req, 'CTP')

        data = contract.toFuturesDB()
        # 获得 tradingDay

        collection = self.mainEngine.dbClient[CONTRACT_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]

        # 检查是否已经存在合约
        oldContract = collection.find_one({'vtSymbol': vtSymbol}, {'_id': 0})
        if not oldContract:
            # 尚未存在新合约,保存
            collection.insert_one(data)

        if not oldContract or oldContract.get('marginRate') is None:
            # 尚未更新保证金率
            self.marginRateBySymbol[vtSymbol] = None
        if not oldContract or oldContract.get('openRatioByMoney') is None:
            # 尚未更新手续费
            self.vtCommissionRateBySymbol[vtSymbol] = None

        self._subcribeNum += 1
        if not self.startReport and self._subcribeNum > 400:
            # 汇报启动
            self.startReport = True
            self.mainEngine.slavemReport.lanuchReport()
            # slavem = vtGlobal.VT_setting['slavem']
            # url = 'mongodb://{username}:{password}@{host}:{port}/{dbn}?authMechanism=SCRAM-SHA-1'.format(
            #     **slavem)
            # try:
            #     # 设置MongoDB操作的超时时间为0.5秒
            #     self.dbClient = pymongo.MongoClient(url, connectTimeoutMS=500)
            #
            #     # 调用server_info查询服务器状态，防止服务器异常并未连接成功
            #     self.dbClient.server_info()
            #
            #     # 提交报告的 collection
            #     report = self.dbClient.slavem['report']
            #     r = {
            #         'name': slavem['name'],
            #         'type': slavem['type'],
            #         'datetime': datetime.datetime.now(LOCAL_TZINFO),
            #         'host': slavem['localhost'],
            #     }
            #
            #     r = report.insert_one(r)
            #     if not r.acknowledged:
            #         print(u'启动汇报失败!')
            #     else:
            #         print(u'启动汇报完成')
            # except:
            #     print(u'启动汇报失败!')
            #     traceback.print_exc()

    # ----------------------------------------------------------------------
    def loadSetting(self):
        """载入设置"""

        with open(self.settingFileName) as f:
            drSetting = json.load(f)

            # 如果working设为False则不启动行情记录功能
            working = drSetting['working']
            if not working:
                return

            if 'tick' in drSetting:
                l = drSetting['tick']
                # setting = ["m1609", "XSPEED"],
                for setting in l:
                    symbol = setting[0]
                    vtSymbol = symbol

                    req = VtSubscribeReq()
                    req.symbol = setting[0]

                    # 针对LTS和IB接口，订阅行情需要交易所代码
                    if len(setting) >= 3:
                        req.exchange = setting[2]
                        vtSymbol = '.'.join([symbol, req.exchange])

                    # 针对IB接口，订阅行情需要货币和产品类型
                    if len(setting) >= 5:
                        req.currency = setting[3]
                        req.productClass = setting[4]

                    self.mainEngine.subscribe(req, setting[1])

                    drTick = DrTickData()  # 该tick实例可以用于缓存部分数据（目前未使用）
                    self.tickDict[vtSymbol] = drTick

            if 'bar' in drSetting:
                l = drSetting['bar']

                for setting in l:
                    symbol = setting[0]
                    vtSymbol = symbol

                    req = VtSubscribeReq()
                    req.symbol = symbol

                    if len(setting) >= 3:
                        req.exchange = setting[2]
                        vtSymbol = '.'.join([symbol, req.exchange])

                    if len(setting) >= 5:
                        req.currency = setting[3]
                        req.productClass = setting[4]

                    self.mainEngine.subscribe(req, setting[1])

                    bar = DrBarData()
                    self.barDict[vtSymbol] = bar

            if 'active' in drSetting:
                d = drSetting['active']

                # 注意这里的vtSymbol对于IB和LTS接口，应该后缀.交易所
                for activeSymbol, vtSymbol in d.items():
                    self.activeSymbolDict[vtSymbol] = activeSymbol

            # 启动数据插入线程
            self.start()

            # 注册事件监听
            self.registerEvent()

            # ----------------------------------------------------------------------

    def procecssTickEvent(self, event):
        """处理行情推送"""
        tick = event.dict_['data']
        vtSymbol = tick.vtSymbol

        # 转化Tic k格式
        drTick = DrTickData()
        d = drTick.__dict__
        for key in d.keys():
            if key != 'datetime':
                d[key] = tick.__getattribute__(key)
        drTick.datetime = LOCAL_TZINFO.localize(
            datetime.datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f'))

        bar = self.barDict.get(vtSymbol)

        # 如果第一个TICK或者新的一分钟
        if not bar.datetime:
            # 刚开盘，没有任何数据
            bar.tickNew(drTick)
        elif bar.datetime != bar.dt2DTM(drTick.datetime):
            # 新的1分钟
            now = LOCAL_TZINFO.localize(datetime.datetime.now())
            if drTick.datetime - now < datetime.timedelta(hours=1):
                # 如果这个 tick 跟当前时间戳差距超过1小时就视为无效
                if bar.vtSymbol:
                    oldBar = copy.copy(bar)
                    # self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)
                    self.insertData(MINUTE_DB_NAME, BAR_COLLECTION_NAME, oldBar)
                    # if vtSymbol in self.activeSymbolDict:
                    #     保存主力合约
                    #     activeSymbol = self.activeSymbolDict[vtSymbol]
                    #     self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

                    self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                                    time=bar.time,
                                                                    open=bar.open,
                                                                    high=bar.high,
                                                                    low=bar.low,
                                                                    close=bar.close))
                bar.tickNew(drTick)
                # 否则继续累加新的K线
            else:
                pass # 忽略这个 tick

        else:
            bar.tickUpdate(drTick)
            # 更新分钟线数据 ================================================================

    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.procecssTickEvent)
        self.eventEngine.register(EVENT_CONTRACT, self.subscribeDrContract)
        self.eventEngine.register(EVENT_MARGIN_RATE, self.updateMariginRate)
        self.eventEngine.register(EVENT_COMMISSION_RATE, self.updateCommissionRate)

    # ----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是CtaTickData或者CtaBarData）"""
        # self.queue.put((dbName, collectionName, data))
        self.queue.put(data)

    # ----------------------------------------------------------------------
    def run(self):
        """运行插入线程"""
        while self.active:
            count = 0
            datas = []
            while True:
                try:
                    data = self.queue.get_nowait()
                    datas.append(data.toSave())
                    count += 1
                except Empty:
                    break
            if datas:
                self.mainEngine.dbInsertMany(MINUTE_DB_NAME, BAR_COLLECTION_NAME, datas)
                self.mainEngine.dbInsertMany(MINUTE_DB_NAME, BAR_COLLECTION_NAME_BAK, datas)

            time.sleep(5)
            # slavem 的心跳
            self.mainEngine.slavemReport.heartBeat()

        self.mainEngine.slavemReport.endHeartBeat()

    # ----------------------------------------------------------------------
    def start(self):
        """启动"""
        self.active = True
        self.thread.start()
        self.threadUpdateContractDetail.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """退出"""
        if self.active:
            self.active = False
            self.thread.join()
            self.threadUpdateContractDetail.join()

    # ----------------------------------------------------------------------
    def writeDrLog(self, content):
        """快速发出日志事件"""
        log = VtLogData()
        log.logContent = content
        event = Event(type_=EVENT_DATARECORDER_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)

    @staticmethod
    def vtSymbol2TickCollectionName(vtSymbol):
        """

        :param vtSymbol:
        :return:
        """
        return '{}_{}'.format(vtSymbol, TICK_COLLECTION_SUBFIX)

    @staticmethod
    def vtSymbol2BarCollectionName(vtSymbol, min=1):
        """

        :param vtSymbol:
        :return:
        """
        return '{}_{}{}'.format(vtSymbol, min, BAR_COLLECTION_SUBFIX)

    def initDRCollection(self):
        """

        :return:
        """
        self.initContractCollection()

        self.initBarCollection(BAR_COLLECTION_NAME)
        self.initBarCollection(BAR_COLLECTION_NAME_BAK)

    def initContractCollection(self):
        if CONTRACT_INFO_COLLECTION_NAME in self.mainEngine.dbClient[MINUTE_DB_NAME].collection_names():
            # colleciton contract 已经存在
            return
        self.mainEngine.dbClient[MINUTE_DB_NAME].create_collection(CONTRACT_INFO_COLLECTION_NAME)

    def initBarCollection(self, barCollectionName):
        if barCollectionName in self.mainEngine.dbClient[MINUTE_DB_NAME].collection_names():
            # colleciton bar_1min 已经存在
            return
        # 创建新的 collection
        self.mainEngine.dbClient[MINUTE_DB_NAME].create_collection(barCollectionName)
        collection = self.mainEngine.dbClient[MINUTE_DB_NAME][barCollectionName]

        indexSymbol = IndexModel([('symbol', ASCENDING)], name='symbol', background=True)
        indexTradingDay = IndexModel([('tradingDay', DESCENDING)], name='tradingDay', background=True)
        collection.create_indexes(
            [
                indexSymbol,
                indexTradingDay,
            ],
        )

    def getMarginRate(self):
        """
        将保证金率更新到合约中
        :return:
        """
        beginTime = datetime.datetime.now()
        turn = 1
        while self.active:
            while not self.marginRateBySymbol:
                time.sleep(1)
                now = datetime.datetime.now()
                if now - beginTime > datetime.timedelta(minutes=1):
                    # 超过10分钟没有新增合约，退出
                    return

            print(u'更新保证金率第 {} 轮 {} 个合约'.format(turn, len(self.marginRateBySymbol)))
            turn += 1
            for symbol, marginRate in list(self.marginRateBySymbol.items()):
                if marginRate is not None:
                    # 已经获取到了保证金率
                    self.marginRateBySymbol.pop(symbol)
                    continue

                count = 0
                while self.marginRateBySymbol[symbol] is None:
                    if count % 12 == 0:
                        print(u'尝试获取 {} 的保证金率'.format(symbol))
                        self.mainEngine.qryMarginRate('CTP', symbol)
                    count += 1
                    time.sleep(0.1)

    def updateMariginRate(self, event):
        """
        更新保证金率
        :param event:
        :return:
        """
        marginRate = event.dict_['data']
        self.marginRateBySymbol[marginRate.vtSymbol] = marginRate.rate

        # 保存到数据库
        collection = self.mainEngine.dbClient[CONTRACT_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]
        collection.find_one_and_update({'vtSymbol': marginRate.vtSymbol}, {'$set': {'marginRate': marginRate.marginRate}})

    def updateCommissionRate(self, event):
        """
        更新保证金率
        :param event:
        :return:
        """
        vtCr = event.dict_['data']

        for vtSymbol in list(self.vtCommissionRateBySymbol.keys()):
            if vtCr.underlyingSymbol == vtSymbol:
                # 返回 rb1801, 合约有变动，强制更新
                self.vtCommissionRateBySymbol[vtSymbol] = vtCr
                return
            elif vtSymbol.startswith(vtCr.underlyingSymbol):
                # 返回 rb ,合约没有变动
                self.vtCommissionRateBySymbol[vtSymbol] = vtCr
                return
            else:
                pass

    def getCommissionRate(self):
        """
        将向后续费率更新到合约中
        :return:
        """
        beginTime = datetime.datetime.now()
        turn = 1

        while self.active:
            while not self.vtCommissionRateBySymbol:
                time.sleep(1)
                now = datetime.datetime.now()
                if now - beginTime > datetime.timedelta(minutes=1):
                    # 超过10分钟没有新增合约，退出
                    return

            print(u'更新手续费率第 {} 轮'.format(turn))
            turn += 1
            for symbol, marginRate in list(self.vtCommissionRateBySymbol.items()):
                if marginRate is not None:
                    # 已经获取到了保证金率
                    self.vtCommissionRateBySymbol.pop(symbol)
                    continue

                count = 0
                # 由于手续费率返回的值可能会更新到其他同品种合约，所以加载之前需要重置
                self.vtCommissionRateBySymbol[symbol] = None

                while self.vtCommissionRateBySymbol[symbol] is None:
                    if count % 12 == 0:
                        print(u'尝试获取 {} 的手续费率'.format(symbol))
                        self.mainEngine.qryCommissionRate('CTP', symbol)
                    count += 1
                    time.sleep(0.1)

                # 将手续费保存到合约中
                vtCr = self.vtCommissionRateBySymbol.pop(symbol)
                # 保存到数据库
                collection = self.mainEngine.dbClient[CONTRACT_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]
                setting = {
                    'openRatioByMoney': vtCr.openRatioByMoney,
                    'closeRatioByMoney': vtCr.closeRatioByMoney,
                    'closeTodayRatioByMoney': vtCr.closeTodayRatioByMoney,

                    'openRatioByVolume': vtCr.openRatioByVolume,
                    'closeRatioByVolume': vtCr.closeRatioByVolume,
                    'closeTodayRatioByVolume': vtCr.closeTodayRatioByVolume,

                }

                collection.find_one_and_update({'vtSymbol': symbol}, {'$set': setting})


    def updateContractDetail(self):
        self.getMarginRate()
        self.getCommissionRate()