# encoding: UTF-8

'''
本文件中实现了CTA策略引擎，针对CTA类型的策略，抽象简化了部分底层接口的功能。

关于平今和平昨规则：
1. 普通的平仓OFFSET_CLOSET等于平昨OFFSET_CLOSEYESTERDAY
2. 只有上期所的品种需要考虑平今和平昨的区别
3. 当上期所的期货有今仓时，调用Sell和Cover会使用OFFSET_CLOSETODAY，否则
   会使用OFFSET_CLOSE
4. 以上设计意味着如果Sell和Cover的数量超过今日持仓量时，会导致出错（即用户
   希望通过一个指令同时平今和平昨）
5. 采用以上设计的原因是考虑到vn.trader的用户主要是对TB、MC和金字塔类的平台
   感到功能不足的用户（即希望更高频的交易），交易策略不应该出现4中所述的情况
6. 对于想要实现4中所述情况的用户，需要实现一个策略信号引擎和交易委托引擎分开
   的定制化统结构（没错，得自己写）
'''

import time
import traceback
import datetime
from itertools import chain
from bson.codec_options import CodecOptions
from threading import Thread, Timer
from collections import defaultdict, OrderedDict

import arrow
from pymongo import IndexModel, ASCENDING, DESCENDING
import tradingtime as tt
from vnpy.trader.vtFunction import LOCAL_TIMEZONE, loadConfigIni

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTickData, VtBarData, VtErrorData
from vnpy.trader.vtGateway import VtSubscribeReq, VtOrderReq, VtCancelOrderReq, VtLogData
from vnpy.trader.vtFunction import todayDate, getJsonPath
from vnpy.trader.app.ctaStrategy.ctaEngine import CtaEngine as VtCtaEngine
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.svtEngine import MainEngine, PositionDetail
from vnpy.trader.vtObject import VtMarginRate, VtCommissionRate

from .ctaBase import *
from .strategy import STRATEGY_CLASS


########################################################################
class CtaEngine(VtCtaEngine):
    """CTA策略引擎"""
    settingFileName = 'CTA_setting.ini'
    settingfilePath = getJsonPath(settingFileName, __file__)

    def __init__(self, mainEngine, eventEngine):
        self.accounts = {}  # {accountID: vtAccount}
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        assert isinstance(self.mainEngine, MainEngine)

        # 历史行情的 collection
        self.mainEngine.dbConnect()

        # 1min bar
        self.ctpCol1minBar = self.mainEngine.ctpdb[MINUTE_COL_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 日线 bar
        self.ctpCol1dayBar = self.mainEngine.ctpdb[DAY_COL_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 合约的详情
        self.contractCol = self.mainEngine.ctpdb[CONTRACT_COL_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        self.strategyDB = self.mainEngine.strategyDB
        # 尝试创建 ctaCollection
        self.initCollection()

        # cta 策略存库
        self.ctaCol = self.mainEngine.strategyDB[CTA_COL_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 持仓存库
        self.posCol = self.mainEngine.strategyDB[POSITION_COLLECTION_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 成交存库
        self.tradeCol = self.mainEngine.strategyDB[TRADE_COLLECTION_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 订单响应存库
        self.orderBackCol = self.mainEngine.strategyDB[ORDERBACK_COLLECTION_NAME].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

        # 心跳相关
        self.heartBeatInterval = 49  # second
        # 将心跳时间设置为1小时候开始
        # report 之后会立即重置为当前触发心跳
        self.nextHeatBeatTime = time.time() + 60 * 10
        self.heatBeatTickCount = 0
        self.nextHeartBeatCount = 100  # second
        self.active = True

        self.positionErrorCountDic = defaultdict(lambda: 0)  # {strategy: errCount}出现仓位异常的策略
        self.checkPositionCount = 0  # 仓位检查间隔计数
        self.checkPositionInterval = 2  # second
        self.reportPosErrCount = 5  # 连续5次仓位异常则报告

        # 维持心跳的品种
        self.heatbeatSymbols = []

        self.vtOrderReqToShow = {}  # 用于展示的限价单对象

        self.stopPriceSlippage = {}  # 记录停止单的触发价，用来记录滑点

        self.strategyByVtSymbol = defaultdict(lambda: set())  # {symbol: set(strategy1, strategy2, ...)}

        # self.waitStopStartTimeDic = defaultdict(lambda: None)  # 开始等待停止单的时间
        # self.waittingVtOrderIDListDic = defaultdict(list)  # 等待成交的停止单触发的订单 {'vtSymbol': vtOrderList}

        if __debug__:
            import pymongo.collection
            assert isinstance(self.ctpCol1dayBar, pymongo.collection.Collection)
            assert isinstance(self.ctpCol1minBar, pymongo.collection.Collection)

    def loadBar(self, vtSymbol, collectionName, barNum, barPeriod=1):
        """
        从数据库中读取历史行情
        :param vtSymbol:
        :param collectionName:  bar_1min  OR bar_1day
        :param barNum: 要加载的 bar 的数量
        :param barPeriod:
        :return:
        """
        collection = {
            'bar_1min': self.ctpCol1minBar,
            'bar_1day': self.ctpCol1dayBar,
        }.get(collectionName)
        # 假设周期 barPeriod=7, barNum=10
        cursor = collection.find({'vtSymbol': vtSymbol}).hint('vtSymbol')
        amount = cursor.count()
        # 先取余数
        rest = amount % barPeriod

        # 总的需要载入的 bar 数量，保证数量的同时，每根bar的周期不会乱掉
        barAmount = barNum * barPeriod + rest

        isTraingTime, loadDate = tt.get_tradingday(arrow.now().datetime)

        loadBarNum = 0
        noDataDays = 0

        documents = []  # [ [day31bar1, day31bar2, ...], ... , [day9bar1, day1bar2, ]]
        while noDataDays <= 30:
            # 连续一个月没有该合约数据，则退出
            sql = {
                'vtSymbol': vtSymbol,
                'tradingDay': loadDate
            }
            # 获取一天的 1min bar
            cursor = collection.find(sql, {'_id': 0})
            count = cursor.count()
            if count != 0:
                # 有数据，加载数据
                noDataDays = 0
                doc = [i for i in cursor]
                doc.sort(key=lambda bar: bar['datetime'])
                documents.append(doc)
                loadBarNum += len(doc)
                if loadBarNum > barAmount:
                    # 数量够了， 跳出循环
                    break
            else:
                # 没有任何数据
                noDataDays += 1
            # 往前追溯
            loadDate -= datetime.timedelta(days=1)

        # 翻转逆序
        documents.reverse()
        documents = list(chain(*documents))  # 衔接成一个 list

        # 加载指定数量barAmount的 bar
        l = []
        timestamp = ((9, 0, 0), (21, 0, 0))
        preKline = None
        for d in documents[-barAmount:]:
            bar = VtBarData()
            bar.load(d)
            if (bar.datetime.hour, bar.datetime.minute, bar.datetime.second) in timestamp:
                preKline = bar
                # self.log.info(u'聚合K线 {} {}'.format(bar.vtSymbol, bar.datetime))
                continue
            if preKline:
                # 聚合到下一根K线上，可能是 9:02 ，不一定是9:01
                bar.high = max(preKline.high, bar.high)
                bar.open = preKline.open
                bar.low = min(preKline.low, bar.low)
                bar.volume += preKline.volume
                preKline = None
            l.append(bar)

        return l

    def callStrategyFunc(self, strategy, func, params=None):
        """调用策略的函数，若触发异常则捕捉"""
        try:
            # self.log.info(u'开盘 tick 没丢失')
            if params:
                func(params)
            else:
                func()
        except Exception:
            # 停止策略，修改状态为未初始化
            strategy.trading = False
            strategy.inited = False
            traceback.print_exc()
            # 发出日志
            preMsg = '策略{}触发异常已停止'.format(strategy.name)
            content = f'{preMsg}\n{traceback.format_exc()}'
            self.log.error(content)

    def processStopOrder(self, tick):
        """收到行情后处理本地停止单（检查是否要立即发出）"""
        vtSymbol = tick.vtSymbol

        # if self.waittingVtOrderIDListDic[tick.vtSymbol]:
        #     # 有等待的停止单
        #     self.log.info(u'{} 停止单锁定中'.format(tick.vtSymbol))
        #     try:
        #         if datetime.datetime.now() - self.waitStopStartTimeDic[tick.vtSymbol] > datetime.timedelta(seconds=5):
        #             self.log.error(u'停止单超过5秒未响应')
        #             self.waitStopStartTime = None
        #             self.waittingVtOrderIDListDic[tick.vtSymbol] = []
        #     except Exception:
        #         self.log.error(traceback.format_exc())
        #
        #     return

        # 首先检查是否有策略交易该合约
        if vtSymbol in self.tickStrategyDict:
            # 遍历等待中的停止单，检查是否会被触发
            for so in self.getAllStopOrdersSorted(vtSymbol):
                if not so.strategy.canProcessStopOrder():
                    # 检查策略是否正在下停止单
                    continue
                if so.vtSymbol == vtSymbol:
                    if so.stopProfile:
                        # 止盈停止单
                        longTriggered = so.direction == DIRECTION_LONG and tick.bidPrice1 <= so.price  # 多头止盈单被触发
                        shortTriggered = so.direction == DIRECTION_SHORT and tick.askPrice1 >= so.price  # 空头止盈单被触发
                    else:
                        # 追价停止单
                        longTriggered = so.direction == DIRECTION_LONG and tick.bidPrice1 >= so.price  # 多头停止单被触发
                        shortTriggered = so.direction == DIRECTION_SHORT and tick.askPrice1 <= so.price  # 空头停止单被触发

                    if longTriggered or shortTriggered:
                        # 买入和卖出分别以涨停跌停价发单（模拟市价单）
                        if so.direction == DIRECTION_LONG:
                            price = tick.upperLimit
                        else:
                            price = tick.lowerLimit

                        # 发出市价委托
                        log = '{} {} {} {} {} {}'.format(so.stopOrderID, so.vtSymbol, so.vtSymbol, so.orderType,
                                                         so.price,
                                                         so.volume)
                        self.log.info('触发停止单 {}'.format(log))

                        if so.volume != 0:
                            so.strategy.setStopOrdering()  # 停止单锁定
                            vtOrderIDList = self.sendOrder(so.vtSymbol, so.orderType, price, so.volume, so.strategy)
                            for vtOrderID in vtOrderIDList:
                                self.log.info(
                                    'stopPriceSlippage - vtOrderID: {} so.price: {}'.format(vtOrderID, so.price))
                                self.stopPriceSlippage[vtOrderID] = so.price
                                so.vtOrderID = vtOrderID

                            # # 将状态设置为有停止单
                            # if vtOrderIDList:
                            #     self.log.info(u'停止单锁定')
                            #     self.waittingVtOrderIDListDic[tick.vtSymbol] = vtOrderIDList
                            #     self.waitStopStartTimeDic[tick.vtSymbol] = datetime.datetime.now()

                        # 从活动停止单字典中移除该停止单
                        del self.workingStopOrderDict[so.stopOrderID]

                        # 从策略委托号集合中移除
                        s = self.strategyOrderDict[so.strategy.name]
                        if so.stopOrderID in s:
                            s.remove(so.stopOrderID)

                        # 更新停止单状态，并通知策略
                        so.status = STOPORDER_TRIGGERED
                        so.strategy.onStopOrder(so)
                        # 每个 tick 最多只触发一个停止单
                        break

    def getAllOrderToShow(self, strategyName):
        """

        :param vtSymbol:
        :return:
        """
        orderList = []
        for orderID in self.strategyOrderDict[strategyName]:
            dic = self.vtOrderReqToShow.get(orderID)
            if dic:
                orderList.append(dic)

        return orderList

    def getAllStopOrderToShow(self, strategyName):
        """
        只展示有效的停止单
        :param vtSymbol:
        :return:
        """
        orderList = []
        for orderID in self.strategyOrderDict[strategyName]:
            so = self.workingStopOrderDict.get(orderID)
            if isinstance(so, StopOrder):
                orderList.append(so)
        return orderList

    def getAllStopOrdersSorted(self, vtSymbol):
        """
        对全部停止单排序后
        :return:
        """
        longOpenStopOrders = []
        shortCloseStopOrders = []
        shortOpenStopOrders = []
        longCloseStopOrders = []
        stopOrders = []
        soBySymbols = [so for so in list(self.workingStopOrderDict.values()) if so.vtSymbol == vtSymbol]

        for so in soBySymbols:
            if so.direction == DIRECTION_LONG:
                if so.offset == OFFSET_OPEN:
                    # 买开
                    longOpenStopOrders.append(so)
                else:
                    # 卖空
                    shortCloseStopOrders.append(so)
            elif so.direction == DIRECTION_SHORT:
                if so.offset == OFFSET_OPEN:
                    # 卖开
                    shortOpenStopOrders.append(so)
                else:
                    # 买空
                    longCloseStopOrders.append(so)
            else:
                stopOrders.append(so)
                self.log.error('未知的停止单方向 {}'.format(so.direction))

        # 根据触发价排序，优先触发更优的
        # 买开
        longOpenStopOrders.sort(key=lambda so: (so.price, so.priority))
        # 平多
        shortCloseStopOrders.sort(key=lambda so: (so.price, -so.priority))
        # 开多
        shortOpenStopOrders.sort(key=lambda so: (so.price, -so.priority))
        shortOpenStopOrders.reverse()
        # 卖空
        longCloseStopOrders.sort(key=lambda so: (so.price, so.priority))
        longCloseStopOrders.reverse()

        stopOrders.extend(shortCloseStopOrders)
        stopOrders.extend(longCloseStopOrders)
        stopOrders.extend(longOpenStopOrders)
        stopOrders.extend(shortOpenStopOrders)

        # # 先撮合平仓单
        # if self.bar.open >= self.bar.close:
        #     # 阴线，撮合优先级 平仓单 > 多单
        #     stopOrders.extend(shortCloseStopOrders)
        #     stopOrders.extend(longCloseStopOrders)
        #     stopOrders.extend(longOpenStopOrders)
        #     stopOrders.extend(shortOpenStopOrders)
        # else:
        #     # 阳线，撮合优先级，平仓单 > 空单
        #     stopOrders.extend(longCloseStopOrders)
        #     stopOrders.extend(shortCloseStopOrders)
        #     stopOrders.extend(shortOpenStopOrders)
        #     stopOrders.extend(longOpenStopOrders)

        return stopOrders

    def saveCtaDB(self, sql, document):
        """
        将 cta 策略的数据保存到数据库
        :return:
        """

        self.ctaCol.find_one_and_update(sql, document, upsert=True)

    def initCollection(self):
        self.createCtaCollection()
        self.createPosCollecdtion()
        self.createTradeCollecdtion()
        self.createOrderBackCollecdtion()

    def createCtaCollection(self):
        db = self.strategyDB

        if __debug__:
            import pymongo.database
            assert isinstance(db, pymongo.database.Database)

        colNames = db.collection_names()
        if CTA_COL_NAME not in colNames:
            # 还没创建 cta collection
            ctaCol = db.create_collection(CTA_COL_NAME)
        else:
            ctaCol = db[CTA_COL_NAME]

        # 创建创建索引
        indexSymbol = IndexModel([('symbol', DESCENDING)], name='symbol', background=True)
        indexClass = IndexModel([('class', ASCENDING)], name='class', background=True)
        indexDatetime = IndexModel([('datetime', DESCENDING)], name='datetime', background=True)

        indexes = [indexSymbol, indexClass, indexDatetime]
        self.mainEngine.createCollectionIndex(ctaCol, indexes)

    def createPosCollecdtion(self):
        db = self.strategyDB

        if __debug__:
            import pymongo.database
            assert isinstance(db, pymongo.database.Database)

        colNames = db.collection_names()
        if POSITION_COLLECTION_NAME not in colNames:
            # 还没创建 cta collection
            col = db.create_collection(POSITION_COLLECTION_NAME)
        else:
            col = db[POSITION_COLLECTION_NAME]

        posMulIndex = [('vtSymbol', DESCENDING), ('name', DESCENDING), ('className', DESCENDING)]

        posIndex = IndexModel(posMulIndex, name='posIndex', background=True, unique=True)
        self.mainEngine.createCollectionIndex(col, [posIndex])

    def createTradeCollecdtion(self):
        """
        成交单存库
        :return:
        """

        db = self.strategyDB

        if __debug__:
            import pymongo.database
            assert isinstance(db, pymongo.database.Database)

        colNames = db.collection_names()
        if TRADE_COLLECTION_NAME not in colNames:
            # 还没创建 cta collection
            col = db.create_collection(TRADE_COLLECTION_NAME)
        else:
            col = db[TRADE_COLLECTION_NAME]

        # 成交单的索引
        indexSymbol = IndexModel([('symbol', DESCENDING)], name='symbol', background=True)
        indexClass = IndexModel([('class', ASCENDING)], name='class', background=True)
        indexDatetime = IndexModel([('datetime', DESCENDING)], name='datetime', background=True)

        indexes = [indexSymbol, indexClass, indexDatetime]
        self.mainEngine.createCollectionIndex(col, indexes)

    def createOrderBackCollecdtion(self):
        """
        成交单存库
        :return:
        """

        db = self.strategyDB

        if __debug__:
            import pymongo.database
            assert isinstance(db, pymongo.database.Database)

        colNames = db.collection_names()
        if ORDERBACK_COLLECTION_NAME not in colNames:
            # 还没创建 cta collection
            col = db.create_collection(ORDERBACK_COLLECTION_NAME)
        else:
            col = db[ORDERBACK_COLLECTION_NAME]

        # 成交单的索引
        indexSymbol = IndexModel([('symbol', DESCENDING)], name='symbol', background=True)
        indexDatetime = IndexModel([('timestamp', DESCENDING)], name='timestamp', background=True)

        indexes = [indexSymbol, indexDatetime]
        self.mainEngine.createCollectionIndex(col, indexes)

    def initAll(self):
        super(CtaEngine, self).initAll()

        strategyList = list(self.strategyDict.values())
        for s in strategyList:
            # 先从合约数据库中获取
            dic = self.contractCol.find_one({'vtSymbol': s.vtSymbol})
            self.loadCommissionRate(s, dic)
            self.loadMarginRate(s, dic)

        strategyList = list(self.strategyDict.values())
        ctpGatway = self.mainEngine.getGateway('CTP')
        for s in strategyList:
            # 更新品种保证金率
            ctpGatway.qryQueue.put((self._qryMarginFromStrategy, (s,)))
            # 查询手续费率
            ctpGatway.qryQueue.put((self._qryCommissionFromStrategy, (s,)))

        self.checkContract()

    def checkContract(self):
        usList = set()
        for s in self.strategyDict.values():
            us = tt.contract2name(s.vtSymbol)
            usList.add(us)

        activeContractDic = {}
        for us in usList:
            # 查找品种的所有合约
            cursor = self.contractCol.find(
                {'underlyingSymbol': us, 'activeEndDate': {'$ne': None}},
                {'_id': 0, 'symbol': 1, 'vtSymbol': 1, 'activeEndDate': 1, 'underlyingSymbol': 1}
                                           )
            contract = [c for c in cursor]
            # 找出主力合约
            activeContract = contract[0]
            for c in contract[1:]:
                if c['activeEndDate'] > activeContract['activeEndDate']:
                    activeContract = c
            # 缓存主力合约
            activeContractDic[activeContract['underlyingSymbol']] = activeContract

        # 对比策略使用的合约是否是主力合约
        for s in self.strategyDict.values():
            us = tt.contract2name(s.vtSymbol)
            vtSymbol = activeContractDic[us]['vtSymbol']
            if s.vtSymbol != vtSymbol:
                self.log.warning(f'{s.name}:{s.vtSymbol} 使用的不是主力合约 {vtSymbol}')

    def loadMarginRate(self, s, dic):
        vm = VtMarginRate()
        vm.loadFromContract(dic)
        s.setMarginRate(vm)
        self.log.debug('预加载保证金率 {} {}'.format(s.vtSymbol, vm.marginRate))

    def loadCommissionRate(self, s, dic):
        vc = VtCommissionRate()
        vc.loadFromContract(dic)
        s.setCommissionRate(vc)
        self.log.debug('预加载手续费率 {}'.format(s.vtSymbol))

    def _qryMarginFromStrategy(self, s):
        """

        :param s: strategy
        :return:
        """
        if not s.isNeedUpdateMarginRate:
            # 不需要更新保证金
            return
        self.log.info('查询保证金 {}'.format(s.vtSymbol))
        self.mainEngine.qryMarginRate('CTP', s.symbol)

        ctpGatway = self.mainEngine.getGateway('CTP')

        ctpGatway.qryQueue.put((self._qryMarginFromStrategy, (s,)))

    def _qryCommissionFromStrategy(self, s):
        """

        :param s: strategy
        :return:
        """
        if not s.isNeedUpdateCommissionRate:
            # 不需要更新手续费
            return
        self.log.info('查询手续费 {}'.format(s.vtSymbol))
        self.mainEngine.qryCommissionRate('CTP', s.symbol)

        ctpGatway = self.mainEngine.getGateway('CTP')

        ctpGatway.qryQueue.put((self._qryCommissionFromStrategy, (s,)))

    def stop(self):
        """
        程序停止时退出前的调用
        :return:
        """
        self.active = False
        self.log.info('CTA engine 即将关闭……')
        self.stopAll()

        self.log.info('停止心跳')
        self.mainEngine.slavemReport.endHeartBeat()

    def savePosition(self, strategy):
        gateWay = self.mainEngine.getGateway('CTP')

        """保存策略的持仓情况到数据库"""
        flt = {'name': strategy.name,
               'className': strategy.className,
               'vtSymbol': strategy.vtSymbol,
               'userID': gateWay.tdApi.userID}

        d = {'name': strategy.name,
             'symbol': strategy.symbol,
             'vtSymbol': strategy.vtSymbol,
             'className': strategy.className,
             'pos': strategy.pos,
             'userID': gateWay.tdApi.userID}

        # self.mainEngine.dbUpdate(POSITION_DB_NAME, POSITION_COLLECTION_NAME,
        #                          d, flt, True)

        self.posCol.replace_one(flt, d, upsert=True)

        content = '策略%s持仓保存成功，当前持仓%s' % (strategy.name, strategy.pos)
        self.log.info(content)
        self.writeCtaLog(content)

    # ----------------------------------------------------------------------
    def loadPosition(self):
        """从数据库载入策略的持仓情况"""
        gateWay = self.mainEngine.getGateway('CTP')

        for strategy in list(self.strategyDict.values()):
            flt = {'name': strategy.name,
                   'className': strategy.className,
                   'vtSymbol': strategy.vtSymbol,
                   'userID': gateWay.tdApi.userID}

            # posData = self.mainEngine.dbQuery(POSITION_DB_NAME, POSITION_COLLECTION_NAME, flt)
            # for d in posData:
            #     strategy.pos = d['pos']
            try:
                strategy.pos = self.posCol.find_one(flt)['pos']
            except TypeError:
                self.log.info('{name} 该策略没有持仓'.format(**flt))

    def loadStrategy(self, setting):
        r = super(CtaEngine, self).loadStrategy(setting)
        for s in list(self.strategyDict.values()):
            self.strategyByVtSymbol[s.vtSymbol].add(s)

        return r

    def startAll(self):
        super(CtaEngine, self).startAll()

        # 启动汇报
        # 通常会提前10分钟启动，至此策略加载完毕处于运作状态
        # 心跳要等到10分钟后开始接受行情才会触发心跳
        now = time.time()
        self.log.info('启动汇报')
        self.mainEngine.slavemReport.lanuchReport()

        def foo():
            self.nextHeatBeatTime = now - 1
            self.heartBeat()

        # 10分钟后开始触发一次心跳
        # 避免因为CTP断掉毫无行情，导致心跳从未开始
        Timer(60 * 10, foo).start()

        # 国债期货可以保证 10:15 ~ 10:30 的心跳，不需要对这个时间段进行处理

    def processTickEvent(self, event):
        """处理行情推送"""
        tick = event.dict_['data']
        # 收到tick行情后，先处理本地停止单（检查是否要立即发出）
        self.processStopOrder(tick)

        # 推送tick到对应的策略实例进行处理
        if tick.vtSymbol in self.tickStrategyDict:
            # tick时间可能出现异常数据，使用try...except实现捕捉和过滤
            try:
                # 添加datetime字段
                if not tick.datetime:
                    tick.datetime = datetime.datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')
            except ValueError:
                err = traceback.format_exc()
                self.log.error(err)
                self.writeCtaLog(err)
                return

            # 逐个推送到策略实例中
            l = self.tickStrategyDict[tick.vtSymbol]
            for strategy in l:
                self.callStrategyFunc(strategy, strategy.updateLastTickTime, tick)
                self.callStrategyFunc(strategy, strategy.onTick, tick)

    def _heartBeat(self, event):
        """
        通过 tick 推送事件来触发心跳
        :param event:
        :return:
        """
        tick = event.dict_['data']
        now = time.time()

        self.heatBeatTickCount += 1

        if self.nextHeatBeatTime < now or self.nextHeartBeatCount <= self.heatBeatTickCount:
            self.nextHeatBeatTime = now + self.heartBeatInterval
            self.heatBeatTickCount = 0
            Thread(name='heartBeat', target=self.heartBeat).start()

    def heartBeat(self):
        self.log.info(u'触发心跳')
        self.mainEngine.slavemReport.heartBeat()

    def writeCtaLog(self, content):
        self.log.info(content)
        super(CtaEngine, self).writeCtaLog(content)

    def loadCtaSettingInit(self):
        """
        加载 .ini 文件的 CTA_setting ，代替 loadSetting 函数
        :return:
        """
        ctaConfig = loadConfigIni(self.settingfilePath)

        activeStrategies = ctaConfig.get('strategies', 'active')
        strategiesList = activeStrategies.strip('"').strip("'").split(',')

        for s in strategiesList:
            if s:
                n = 'strategy_{}'.format(s)
                setting = dict(ctaConfig.typeitems(n))
                self.loadStrategy(setting)

        self.loadPosition()

    def loadSetting(self):
        # super(CtaEngine, self).loadSetting()
        self.loadCtaSettingInit()
        for us in ['ag', 'T']:
            # 订阅 ag 和 T 的主力合约
            sql = {
                'underlyingSymbol': us,
                'activeEndDate': {'$ne': None}
            }
            # 逆序, 取出第一个，就是当前的主力合约
            cursor = self.contractCol.find(sql).sort('activeEndDate', -1)
            d = next(cursor)
            vtSymbol = d['vtSymbol']
            symbol = d['symbol']

            # 订阅合约
            contract = self.mainEngine.getContract(vtSymbol)
            if not contract:
                err = '找不到维持心跳的合约 {}'.format(vtSymbol)
                self.log.critical(err)
                time.sleep(1)
                raise ValueError(err)

            self.log.info('订阅维持心跳的合约 {}'.format(vtSymbol))
            self.heatbeatSymbols.append(vtSymbol)

            self.reSubscribe(vtSymbol)

            # 仅对 ag 和 T 的tick推送进行心跳
            self.eventEngine.register(EVENT_TICK + vtSymbol, self._heartBeat)

    def reSubscribe(self, vtSymbol):
        req = VtSubscribeReq()
        contract = self.mainEngine.getContract(vtSymbol)
        req.symbol = contract.symbol
        req.exchange = contract.exchange

        self.mainEngine.subscribe(req, contract.gatewayName)

    def sendStopOrder(self, vtSymbol, orderType, price, volume, strategy, stopProfile=False):
        log = '{} 停止单 {} {} {} {} {}'.format(vtSymbol, strategy.name, orderType, price, volume, stopProfile)
        if volume == 0:
            self.log.warning('下单手数为0 {}'.format(log))
        else:
            self.log.info(log)
        return super(CtaEngine, self).sendStopOrder(vtSymbol, orderType, price, volume, strategy, stopProfile)

    def sendOrder(self, vtSymbol, orderType, price, volume, strategy):
        log = '{} 限价单 {} {} {} {} '.format(vtSymbol, strategy.name, orderType, price, volume)
        if volume == 0:
            self.log.warning(log)
        else:
            self.log.info(log)
        vtOrderIDList = []
        count = 0
        while not vtOrderIDList and count <= 2:
            count += 1
            vtOrderIDList = super(CtaEngine, self).sendOrder(vtSymbol, orderType, price, volume, strategy)
            if not vtOrderIDList:
                time.sleep(3)

        if not vtOrderIDList:
            self.log.warning('vtOrderID 为空，检查是否有限价单无法自动撤单\n{}'.format(log))

        contract = self.mainEngine.getContract(vtSymbol)
        _price = self.roundToPriceTick(contract.priceTick, price)
        for vtOrderID in vtOrderIDList:
            if vtOrderID:
                odic = OrderedDict((
                    ('vtOrderID', vtOrderID),
                    ('vtSymbol', vtSymbol),
                    ('orderType', orderType),
                    ('price', _price),
                    ('volume', volume),
                ))
                self.vtOrderReqToShow[vtOrderID] = odic

        return vtOrderIDList

    def saveTrade(self, dic):
        """
        将成交单保存到数据库
        :param dic:
        :return:
        """
        self.tradeCol.insert_one(dic)

    def saveOrderback(self, dic):
        """
        将订单响应保存到数据库
        :param dic:
        :return:
        """
        self.orderBackCol.insert_one(dic)

    def processTradeEvent(self, event):
        trade = event.dict_['data']

        if trade.stopPrice is None:
            trade.stopPrice = self.stopPriceSlippage.get(trade.vtOrderID)

        super(CtaEngine, self).processTradeEvent(event)

        # 在完成 strategy.pos 的更新后，保存 trade。trade 也保存更新后的 pos
        if trade.vtOrderID in self.orderStrategyDict:
            self.saveTradeByStrategy(trade)

    def saveTradeByStrategy(self, trade):
        strategy = self.orderStrategyDict[trade.vtOrderID]

        dic = trade.__dict__.copy()
        dic.pop('rawData')
        dic['splippage'] = trade.splippage

        # 时间戳
        dt = dic['datetime']

        if not dt.tzinfo:
            t = '成交单 {} {} 没有时区'.format(trade.symbol, dt)
            raise ValueError(t)
        td = dic['tradingDay']
        if td is None:
            t = '成交单 {} {} 没有交易日'.format(trade.symbol, dt)
            raise ValueError(t)
        dic['class'] = strategy.className
        dic['name'] = strategy.name
        dic['pos'] = strategy.pos

        self.saveTrade(dic)
        # 监控滑点问题
        strategy.monitorSplippage(trade)

    def checkPositionDetail(self, event):
        """
        定时检查持仓状况
        :return:
        """

        self.checkPositionCount += 1
        if self.checkPositionCount >= self.checkPositionInterval:
            # 间隔达到5秒
            self.checkPositionCount = 0
        else:
            # 间隔时间不够
            return

        for vtSymbol, strategySet in list(self.strategyByVtSymbol.items()):
            self._checkPositionBySymbol(vtSymbol, strategySet)

            # for s in self.strategyDict.values():
            #     # 上次检查已经有异常了,这次有异常直接回报
            #     self._checkPositionByStrategy(s)

    def _checkPositionBySymbol(self, vtSymbol, strategySet):
        countDic = self.positionErrorCountDic

        def errorHandler(err):
            countDic[vtSymbol] += 1
            if countDic[vtSymbol] >= self.reportPosErrCount:
                err = '仓位异常 停止交易 {}'.format(err)
                for s in strategySet:
                    s.positionErrReport(err)
                    s.trading = False
                    # 全部撤单
                    s.cancelAll()
            else:
                self.log.info('仓位出现异常次数 {}'.format(countDic[vtSymbol]))
                self.log.info('{}'.format(err))

        # 仓位详情
        detail = self.mainEngine.dataEngine.getPositionDetail(vtSymbol)
        posAmount = sum([s.pos for s in strategySet])

        if detail.longPos != detail.longYd + detail.longTd:
            # 多头仓位异常
            err = '{name} longPos:{longPos} longYd:{longYd} longTd:{longTd}'.format(name=vtSymbol, **detail.__dict__)
            errorHandler(err)

        elif detail.shortPos != detail.shortYd + detail.shortTd:
            # 空头仓位异常
            err = '{name} shortPos:{shortPos} shortYd:{shortYd} shortTd:{shortTd}'.format(name=vtSymbol,
                                                                                          **detail.__dict__)
            errorHandler(err)

        elif posAmount != detail.longPos - detail.shortPos:
            err = '{name} posAmount:{posAmount} longPos:{longPos} shortPos:{shortPos} '.format(name=vtSymbol,
                                                                                               posAmount=posAmount,
                                                                                               **detail.__dict__)
            errorHandler(err)
        else:
            # 没有异常，重置仓位异常次数
            countDic[vtSymbol] = 0

    def _checkPositionByStrategy(self, strategy):
        """
        对指定的策略进行仓位检查
        :param strategy:
        :return:
        """
        countDic = self.positionErrorCountDic
        s = strategy
        if s.trading and s.inited:
            # 需要策略已经初始化完且开始交易才进行仓位校验
            pass
        else:
            return

        def errorHandler(err):
            countDic[s] += 1
            if countDic[s] >= self.reportPosErrCount:
                err = '仓位异常 停止交易 {}'.format(err)
                s.positionErrReport(err)
                s.trading = False
                # 全部撤单
                s.cancelAll()
            else:
                self.log.info('仓位出现异常次数 {}'.format(countDic[s]))
                self.log.info('{}'.format(err))

        d = self.mainEngine.dataEngine.getPositionDetail(s.vtSymbol)
        assert isinstance(d, PositionDetail)

        if d.longPos != d.longYd + d.longTd:
            # 多头仓位异常
            err = '{name} longPos:{longPos} longYd:{longYd} longTd:{longTd}'.format(name=s.name, **d.__dict__)
            errorHandler(err)

        elif d.shortPos != d.shortYd + d.shortTd:
            # 空头仓位异常
            err = '{name} shortPos:{shortPos} shortYd:{shortYd} shortTd:{shortTd}'.format(name=s.name,
                                                                                          **d.__dict__)
            errorHandler(err)

        elif s.pos != d.longPos - d.shortPos:
            err = '{name} s.pos:{pos} longPos:{longPos} shortPos:{shortPos} '.format(name=s.name, pos=s.pos,
                                                                                     **d.__dict__)
            errorHandler(err)
        else:
            # 没有异常，重置仓位异常次数
            countDic[s] = 0

    def registerEvent(self):
        super(CtaEngine, self).registerEvent()
        self.eventEngine.register(EVENT_TIMER, self.checkPositionDetail)
        self.eventEngine.register(EVENT_ACCOUNT, self.updateAccount)
        self.eventEngine.register(EVENT_ERROR, self.processOrderError)

    def processOrderEvent(self, event):
        order = event.dict_['data']
        dic = order.__dict__.copy()
        dic['datetime'] = arrow.now().datetime
        try:
            # rawData 会导致无法存库
            dic.pop('rawData')
        except KeyError:
            pass
        self.saveOrderback(dic)

        r = super(CtaEngine, self).processOrderEvent(event)

        # if order.vtOrderID in self.waittingVtOrderIDListDic[order.vtSymbol]:
        #     if order.status == STATUS_ALLTRADED:
        #         self.log.info(u'{} 停止单锁定解除'.format(order.vtSymbol))
        #         self.waittingVtOrderIDListDic[order.vtSymbol].remove(order.vtOrderID)

        return r

    def updateAccount(self, event):
        account = event.dict_['data']
        self.accounts[account.vtAccountID] = account

    def accountToHtml(self):
        datas = []
        for account in list(self.accounts.values()):
            dic = account.__dict__.copy()
            if dic['balance'] != 0:
                dic['marginRate'] = dic['margin'] / dic['balance']
            datas.append(dic)

        return datas

    def cancelOrder(self, vtOrderID):
        self.log.info('撤限价单 {}'.format(vtOrderID))
        try:
            self.vtOrderReqToShow.pop(vtOrderID)
        except KeyError:
            pass
        req = super(CtaEngine, self).cancelOrder(vtOrderID)
        if req is not None:
            self.log.info('{} orderID:{}'.format(req.symbol, req.orderID))
        return req

    def cancelStopOrder(self, stopOrderID):
        """撤销停止单"""
        self.log.info('撤停止单')
        so = super(CtaEngine, self).cancelStopOrder(stopOrderID)
        if so:
            self.log.info('{} orderID:{}'.format(so.vtSymbol, so.stopOrderID))

    def processOrderError(self, event):
        """

        :param event:
        :return:
        """
        return
        err = event.dict_['data']
        assert isinstance(err, VtErrorData)

        log = '报单错误\n'
        log += 'errorID:{}\n'.format(err.errorID)
        log += 'errorMsg:{}\n'.format(err.errorMsg)
        log += 'additionalInfo:{}\n'.format(err.additionalInfo)
        log += 'errorTime:{}\n'.format(err.errorTime)
        self.log.error(log)
