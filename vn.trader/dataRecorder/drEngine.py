# encoding: UTF-8

'''
本文件中实现了行情数据记录引擎，用于汇总TICK数据，并生成K线插入数据库。

使用DR_setting.json来配置需要收集的合约，以及主力合约代码。
'''

import json
import os
import copy
from collections import OrderedDict
from datetime import datetime, timedelta
from Queue import Queue
from threading import Thread
from pymongo.errors import OperationFailure
import traceback

import tradingtime
import pymongo
import vtGlobal

from eventEngine import *
from vtGateway import VtSubscribeReq, VtLogData
from drBase import *
from vtFunction import todayDate
from language import text


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
        self.tickCache = {}  # 缓存队列 {'collcectionName': Queue()}
        # self.queue = Queue()  # 队列
        self.thread = Thread(target=self.run)  # 线程

        # 启动标志
        self._subcribeNum = 0
        self.startReport = False

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

        # ====================================================
        # 创建collection，并设置索引
        db = self.mainEngine.dbClient[CONTRACT_DB_NAME]

        if not self.collectionNames:
            self.collectionNames = set(db.collection_names())

        names = self.collectionNames
        tickColName = self.vtSymbol2TickCollectionName(vtSymbol)
        barColName = self.vtSymbol2BarCollectionName(vtSymbol, min=1)

        names = [tickColName]
        # names = [tickColName, barColName]
        for n in names:
            if n not in names:
                try:
                    # 创建数据库
                    self.writeDrLog(u'创建数据库 {}'.format(n))
                    col = db.create_collection(n)
                    # # 创建索引
                    # r = col.create_index('datetime', unique=True)
                except OperationFailure as e:
                    if e.message == 'collection test already exists':
                        pass
                    else:
                        raise
            else:
                pass
                print(u'已经存在数据库 {}'.format(n))
        # ====================================================

        data = contract.toFuturesDB()
        # 获得 tradingDay
        isAD, tradingDay = tradingtime.get_tradingday(datetime.now())

        collection = self.mainEngine.dbClient[CONTRACT_DB_NAME][CONTRACT_INFO_COLLECTION_NAME]

        tradingDay = tradingDay.strftime('%Y%m%d')
        # 对比差异
        isChange = False
        try:
            oldContract = collection.find({'vtSymbol': vtSymbol}, {'_id': 0}).sort('TradingDay', pymongo.DESCENDING).limit(1).next()
            oldTradingDay = oldContract['TradingDay']
            for k, v in oldContract.items():
                if v != data[k]:
                    # 合约内容有变换
                    isChange = True
                    print(u'{}合约的字段{}存在不一致 oldContract:{} data:{}'.format(symbol, k, v, data[k]))
                    break
        except StopIteration:
            print(u'{}合约因为 StopIteration 不一致')
            isChange = True
        except OperationFailure:
            # 没有数据
            print(u'{}合约因为 OperationFailure 不一致')
            isChange = True

        data['TradingDay'] = tradingDay
        if isChange:
            # 合约有变换，插入一条新的
            collection.insert_one(data)
        else:
            # 没变化，直接更新
            sql = {'vtSymbol': vtSymbol, 'TradingDay': oldTradingDay}
            r = collection.find_one_and_update(sql, {'$set': {'TradingDay': tradingDay}})

        self._subcribeNum += 1
        if not self.startReport and self._subcribeNum > 400:
            # 汇报启动
            self.startReport = True
            url = 'mongodb://{slavemUsername}:{slavemPassword}@{slavemHost}:{slavemPort}/{slavemdbn}?authMechanism=SCRAM-SHA-1'.format(
                **vtGlobal.VT_setting)
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                self.dbClient = pymongo.MongoClient(url, connectTimeoutMS=500)

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                # 提交报告的 collection
                report = self.dbClient.slavem['report']
                r = {
                    'name': vtGlobal.VT_setting['slavemName'],
                    'type': vtGlobal.VT_setting['slavemType'],
                    'datetime': datetime.now(),
                    'host': vtGlobal.VT_setting['slaveMLocalhost'],
                }

                r = report.insert_one(r)
                if not r.acknowledged:
                    print(u'启动汇报失败!')
                else:
                    print(u'启动汇报完成')
            except:
                print(u'启动汇报失败!')
                traceback.print_exc()


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
        drTick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')

        # 更新Tick数据 ====================
        # if vtSymbol in self.tickDict:
        tickColName = self.vtSymbol2TickCollectionName(vtSymbol)
        barColName = self.vtSymbol2BarCollectionName(vtSymbol, min=1)

        # self.insertData(TICK_DB_NAME, vtSymbol, drTick)
        self.insertData(TICK_DB_NAME, tickColName, drTick)

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
        # bar = self.barDict.get(vtSymbol, DrBarData())
        # # 如果第一个TICK或者新的一分钟
        # if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
        #     if bar.vtSymbol:
        #         newBar = copy.copy(bar)
        #         # self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)
        #         self.insertData(MINUTE_DB_NAME, barColName, newBar)
        #
        #         if vtSymbol in self.activeSymbolDict:
        #             activeSymbol = self.activeSymbolDict[vtSymbol]
        #             self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)
        #
        #         self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
        #                                                         time=bar.time,
        #                                                         open=bar.open,
        #                                                         high=bar.high,
        #                                                         low=bar.low,
        #                                                         close=bar.close))
        #     bar.tickNew(drTick)
        #     # 否则继续累加新的K线
        # else:
        #     bar.tickUpdate(drTick)
        # 更新分钟线数据 ================================================================

    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.procecssTickEvent)
        self.eventEngine.register(EVENT_CONTRACT, self.subscribeDrContract)

    # ----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是CtaTickData或者CtaBarData）"""
        # self.queue.put((dbName, collectionName, data.__dict__))
        try:
            q = self.tickCache[collectionName]
        except KeyError:
            q = Queue()
            self.tickCache[collectionName] = q

        # 将tick数据放入队列
        q.put(data)

    # ----------------------------------------------------------------------
    def run(self):
        """运行插入线程"""
        while self.active:
            dbName = TICK_DB_NAME
            for collectionName, q in self.tickCache.items():
                ticks = []
                assert isinstance(q, Queue)
                try:
                    while True:
                        data = q.get_nowait()
                        ticks.append(data.__dict__)
                except Empty:
                    pass
                if ticks:
                    # 批量存储
                    self.mainEngine.dbInsertMany(dbName, collectionName, ticks)

    # ----------------------------------------------------------------------
    def start(self):
        """启动"""
        self.active = True
        self.thread.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """退出"""
        if self.active:
            self.active = False
            self.thread.join()

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
