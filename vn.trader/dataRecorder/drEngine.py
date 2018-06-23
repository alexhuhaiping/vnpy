# encoding: UTF-8

'''
本文件中实现了行情数据记录引擎，用于汇总TICK数据，并生成K线插入数据库。

使用DR_setting.json来配置需要收集的合约，以及主力合约代码。
'''

import logging
import copy
import json
import os
import time
from datetime import datetime
import tradingtime as tt

import arrow
from pymongo import IndexModel, ASCENDING, DESCENDING
from vtFunction import todayDate

from drBase import *
from eventEngine import *
from language import text
from vtGateway import VtSubscribeReq, VtLogData
import vtGlobal


########################################################################
class DrEngine(object):
    """数据记录引擎"""

    settingFileName = 'DR_setting.json'
    path = os.path.abspath(os.path.dirname(__file__))
    settingFileName = os.path.join(path, settingFileName)

    # ----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.log = logging.getLogger('dr')
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
        self.startReport = False

        # 加载全部合约完毕
        self.loadContractDone = False

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

        if contract.last:
            # 汇报启动
            self.startReport = True
            self.mainEngine.slavemReport.lanuchReport()
            self.loadContractDone = True
            self.log.info(u'加载合约完成')

        if contract.productClass != u'期货':
            return
        vtSymbol = symbol = contract.symbol

        self.log.debug(u'订阅 {}'.format(vtSymbol))

        # 检查 tradingtime 是否已经添加了该品种
        try:
            tt.get_trading_status(vtSymbol)
        except TypeError:
            self.log.warning(u'tradingtime 缺少品种 {}'.format(vtSymbol))
            return

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
        is_tradingtime, tradeday = tt.get_tradingday(arrow.now().datetime)
        if not oldContract:
            # 尚未存在新合约,保存
            data['startDate'] = tradeday
            data['endDate'] = tradeday
            collection.insert_one(data)
        else:
            # 已经存在的合约，更新 endDate
            collection.update_one({'vtSymbol': vtSymbol}, {'$set': {'endDate': tradeday}})

        # 尚未更新保证金率
        self.marginRateBySymbol[vtSymbol] = None
        # 尚未更新手续费
        self.vtCommissionRateBySymbol[vtSymbol] = None

    # ----------------------------------------------------------------------
    def loadSetting(self):
        """载入设置"""

        # with open(self.settingFileName) as f:
        with open(vtGlobal.VT_setting['DR_setting']) as f:
            drSetting = json.load(f)

            # 如果working设为False则不启动行情记录功能
            working = drSetting['working']
            if not working:
                return

            if 'tick' in drSetting:
                l = drSetting['tick']
                # setting = ["m1609", "XSPEED"],
                self.log.debug(str(l))
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

        # 更新Tick数据 ====================
        # if vtSymbol in self.tickDict:
        # tickColName = self.vtSymbol2TickCollectionName(vtSymbol)
        # barColName = self.vtSymbol2BarCollectionName(vtSymbol, min=1)

        # self.insertData(TICK_DB_NAME, vtSymbol, drTick)
        # self.insertData(TICK_DB_NAME, tickColName, drTick)
        # self.insertData('ctp', 'tick', drTick)

        # if vtSymbol in self.activeSymbolDict:
        #     activeSymbol = self.activeSymbolDict[vtSymbol]
        #     self.insertData(TICK_DB_NAME, activeSymbol, drTick)

        # 发出日志
        # self.writeDrLog(text.TICK_LOGGING_MESSAGE.format(symbol=drTick.vtSymbol,
        #                                                  time=drTick.time,
        #                                                  last=drTick.lastPrice,
        #                                                  bid=drTick.bidPrice1,
        #                                                  ask=drTick.askPrice1))
        # 更新Tick数据 ====================

        # 更新分钟线数据 ================================================================
        #
        # if vtSymbol in self.barDict:
        bar = self.barDict.get(vtSymbol)

        # 如果第一个TICK或者新的一分钟
        if not bar.datetime:
            # 刚开盘，没有任何数据
            bar.tickNew(drTick)
        elif bar.datetime != bar.dt2DTM(drTick.datetime):
            # 新的1分钟

            if bar.vtSymbol:
                oldBar = copy.copy(bar)
                # self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)
                self.insertData(MINUTE_DB_NAME, BAR_COLLECTION_NAME, oldBar)
                # if vtSymbol in self.activeSymbolDict:
                #     保存主力合约
                #     activeSymbol = self.activeSymbolDict[vtSymbol]
                #     self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

                barText = text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                          time=bar.time,
                                                          open=bar.open,
                                                          high=bar.high,
                                                          low=bar.low,
                                                          close=bar.close)
                self.log.debug(barText)
                self.writeDrLog(barText)
            bar.tickNew(drTick)
            # 否则继续累加新的K线
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
        if CONTRACT_INFO_COLLECTION_NAME not in self.mainEngine.dbClient[MINUTE_DB_NAME].collection_names():
            # colleciton contract 还未创建,先创建
            self.mainEngine.dbClient[MINUTE_DB_NAME].create_collection(CONTRACT_INFO_COLLECTION_NAME)

        collection = self.mainEngine.dbClient[MINUTE_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]

        indexDic = collection.index_information()
        indexes = []
        if 'symbol' not in indexDic:
            indexes.append(
                IndexModel([('symbol', ASCENDING)], name='symbol', background=True)
            )
        if 'underlyingStymbol' not in indexDic:
            indexes.append(
                IndexModel([('underlyingStymbol', ASCENDING)], name='underlyingStymbol', background=True)
            )
        for index in indexes:
            collection.create_indexes([index])

    def initBarCollection(self, barCollectionName):
        if barCollectionName not in self.mainEngine.dbClient[MINUTE_DB_NAME].collection_names():
            # colleciton bar_1min 创建新的 collection
            self.mainEngine.dbClient[MINUTE_DB_NAME].create_collection(barCollectionName)

        collection = self.mainEngine.dbClient[MINUTE_DB_NAME][barCollectionName]
        indexDic = collection.index_information()
        indexes = []

        if 'symbol' not in indexDic:
            indexes.append(
                IndexModel([('symbol', ASCENDING)], name='symbol', background=True)
            )
        if 'tradingDay' not in indexDic:
            indexes.append(
                IndexModel([('tradingDay', ASCENDING)], name='tradingDay', background=True)
            )
        for index in indexes:
            collection.create_indexes([index])

    def getMarginRate(self):
        """
        将保证金率更新到合约中
        :return:
        """
        self.log.info(u'开始更新保证金')
        while self.active:
            while not self.loadContractDone:
                time.sleep(1)

            self.log.info(u'更新保证金率 {} 个合约'.format(len(self.marginRateBySymbol)))
            for symbol, marginRate in list(self.marginRateBySymbol.items()):
                if marginRate is None:
                    time.sleep(1.1)
                    self.log.info(u'尝试获取 {} 的保证金率'.format(symbol))
                    self.mainEngine.qryMarginRate('CTP', symbol)
            else:
                # 全部品种都已经获得保证金
                break

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
        self.log.info(u'更新保证金 {} {}'.format(marginRate.vtSymbol, marginRate.marginRate))

        collection.update_one({'vtSymbol': marginRate.vtSymbol},
                              {'$set': {'marginRate': marginRate.marginRate}})

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
                if self.vtCommissionRateBySymbol[vtSymbol] is None:
                    self.vtCommissionRateBySymbol[vtSymbol] = vtCr
                return
            else:
                pass

    def getCommissionRate(self):
        """
        将向后续费率更新到合约中
        :return:
        """
        while self.active:
            self.log.info(u'更新手续费率')

            while not self.loadContractDone:
                time.sleep(1)

            for symbol, rate in list(self.vtCommissionRateBySymbol.items()):
                if rate is None:
                    self.log.info(u'尝试获取 {} 的手续费率'.format(symbol))
                    self.mainEngine.qryCommissionRate('CTP', symbol)
                    time.sleep(1.1)
            else:
                # 已经全部获取到了手续费
                break

        self.log.info(u'更新保证金到数据库')
        collection = self.mainEngine.dbClient[CONTRACT_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]
        for symbol, vtCr in self.vtCommissionRateBySymbol.items():
            if vtCr is None:
                # 为什么而这里会是None
                continue
            # TODO 将手续费保存到合约中
            setting = {
                'openRatioByMoney': vtCr.openRatioByMoney,
                'closeRatioByMoney': vtCr.closeRatioByMoney,
                'closeTodayRatioByMoney': vtCr.closeTodayRatioByMoney,

                'openRatioByVolume': vtCr.openRatioByVolume,
                'closeRatioByVolume': vtCr.closeRatioByVolume,
                'closeTodayRatioByVolume': vtCr.closeTodayRatioByVolume,

            }
            # 将手续费保存到合约中
            collection.update_one({'vtSymbol': symbol}, {'$set': setting})

    def updateContractDetail(self):
        self.getCommissionRate()
        self.getMarginRate()
