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

from __future__ import division

import time
import traceback
import datetime
from itertools import chain
from bson.codec_options import CodecOptions
from threading import Thread, Timer
from collections import defaultdict

import arrow
from pymongo import IndexModel, ASCENDING, DESCENDING
import tradingtime as tt
from vnpy.trader.vtFunction import LOCAL_TIMEZONE

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTickData, VtBarData
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

    def __init__(self, mainEngine, eventEngine):
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

        if __debug__:
            import pymongo.collection
            assert isinstance(self.ctpCol1dayBar, pymongo.collection.Collection)
            assert isinstance(self.ctpCol1minBar, pymongo.collection.Collection)

    def loadBar(self, symbol, collectionName, barNum, barPeriod=1):
        """
        从数据库中读取历史行情
        :param symbol:
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
        cursor = self.ctpCol1minBar.find({'symbol': symbol}).hint('symbol')
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
                'symbol': symbol,
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
                loadBarNum += cursor.count()
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
        for d in documents[-barAmount:]:
            bar = VtBarData()
            bar.load(d)
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
            preMsg = u'策略{}触发异常已停止'.format(strategy.name)
            errMsg = traceback.format_exc()
            content = u'{}\n{}'.format(preMsg, errMsg.decode('utf-8'))
            self.log.error(content)

    def processStopOrder(self, tick):
        """收到行情后处理本地停止单（检查是否要立即发出）"""
        vtSymbol = tick.vtSymbol

        # 首先检查是否有策略交易该合约
        if vtSymbol in self.tickStrategyDict:
            # 遍历等待中的停止单，检查是否会被触发
            for so in self.getAllStopOrdersSorted(vtSymbol):
                if so.vtSymbol == vtSymbol:
                    longTriggered = so.direction == DIRECTION_LONG and tick.lastPrice >= so.price  # 多头停止单被触发
                    shortTriggered = so.direction == DIRECTION_SHORT and tick.lastPrice <= so.price  # 空头停止单被触发

                    if longTriggered or shortTriggered:
                        # 买入和卖出分别以涨停跌停价发单（模拟市价单）
                        if so.direction == DIRECTION_LONG:
                            price = tick.upperLimit
                        else:
                            price = tick.lowerLimit

                        # 发出市价委托
                        self.sendOrder(so.vtSymbol, so.orderType, price, so.volume, so.strategy)

                        # 从活动停止单字典中移除该停止单
                        del self.workingStopOrderDict[so.stopOrderID]

                        # 从策略委托号集合中移除
                        s = self.strategyOrderDict[so.strategy.name]
                        if so.stopOrderID in s:
                            s.remove(so.stopOrderID)

                        # 更新停止单状态，并通知策略
                        so.status = STOPORDER_TRIGGERED
                        so.strategy.onStopOrder(so)

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
        soBySymbols = [so for so in self.workingStopOrderDict.values() if so.vtSymbol == vtSymbol]

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
                self.log.error(u'未知的停止单方向 {}'.format(so.direction))

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

        # 查询手续费率
        t = Thread(target=self._updateQryCommissionRate)
        t.setDaemon(True)
        t.start()

        # 加载品种保证金率
        t = Thread(target=self._updateQryMarginRate)
        t.setDaemon(True)
        t.start()

    def loadMarginRate(self, s, dic):

        vm = VtMarginRate()
        vm.loadFromContract(dic)
        s.setMarginRate(vm)
        self.log.debug(u'预加载保证金率 {} {}'.format(s.vtSymbol, vm.marginRate))

    def loadCommissionRate(self, s, dic):
        vc = VtCommissionRate()
        vc.loadFromContract(dic)
        s.setCommissionRate(vc)
        self.log.debug(u'预加载手续费率 {}'.format(s.vtSymbol))

    def _updateQryMarginRate(self):
        strategyList = list(self.strategyDict.values())
        for s in strategyList:
            # 再从CTP中更新
            count = 1
            while s.isNeedUpdateMarginRate and self.active:
                if count % 3000 == 0:
                    # 30秒超时
                    err = u'加载品种 {} 保证金率失败'.format(s.vtSymbol)
                    self.log.warning(err)
                    continue

                if count % 30 == 0:
                    # 每3秒重新发送一次
                    # self.log.info(u'尝试加载 {} 保证金率'.format(s.vtSymbol))
                    self.mainEngine.qryMarginRate('CTP', s.vtSymbol)

                # 每0.1秒检查一次返回结果
                time.sleep(0.1)
                count += 1
            else:
                self.log.info(u'加载品种 {} 保证金率成功'.format(s.vtSymbol))

    def _updateQryCommissionRate(self):
        strategyList = list(self.strategyDict.values())
        for s in strategyList:
            # 再从CTP中更新
            count = 1

            while s.isNeedUpdateCommissionRate and self.active:
                if count % 3000 == 0:
                    # 30秒超时
                    self.log.warning(u'加载品种 {} 手续费率超时'.format(str(s.vtSymbol)))
                    continue

                if count % 30 == 0:
                    # 每3秒重新发送一次
                    # self.log.info(u'尝试加载 {} 手续费率'.format(s.vtSymbol))
                    self.mainEngine.qryCommissionRate('CTP', s.vtSymbol)

                # 每0.1秒检查一次返回结果
                time.sleep(0.1)
                count += 1
            else:
                self.log.info(u'加载品种 {} 手续费率成功'.format(str(s.vtSymbol)))

    def stop(self):
        """
        程序停止时退出前的调用
        :return:
        """
        self.active = False
        self.log.info(u'CTA engine 即将关闭……')
        self.stopAll()

        self.log.info(u'停止心跳')
        self.mainEngine.slavemReport.endHeartBeat()

    def savePosition(self, strategy):
        """保存策略的持仓情况到数据库"""
        flt = {'name': strategy.name,
               'className': strategy.className,
               'vtSymbol': strategy.vtSymbol}

        d = {'name': strategy.name,
             'vtSymbol': strategy.vtSymbol,
             'className': strategy.className,
             'pos': strategy.pos}

        # self.mainEngine.dbUpdate(POSITION_DB_NAME, POSITION_COLLECTION_NAME,
        #                          d, flt, True)

        self.posCol.replace_one(flt, d, upsert=True)

        content = u'策略%s持仓保存成功，当前持仓%s' % (strategy.name, strategy.pos)
        self.log.info(content)
        self.writeCtaLog(content)

    # ----------------------------------------------------------------------
    def loadPosition(self):
        """从数据库载入策略的持仓情况"""
        for strategy in self.strategyDict.values():
            flt = {'name': strategy.name,
                   'className': strategy.className,
                   'vtSymbol': strategy.vtSymbol}

            # posData = self.mainEngine.dbQuery(POSITION_DB_NAME, POSITION_COLLECTION_NAME, flt)
            # for d in posData:
            #     strategy.pos = d['pos']
            try:
                strategy.pos = self.posCol.find_one(flt)['pos']
            except TypeError:
                self.log.info(u'{name} 该策略没有持仓'.format(**flt))

    def startAll(self):
        super(CtaEngine, self).startAll()

        # 启动汇报
        # 通常会提前10分钟启动，至此策略加载完毕处于运作状态
        # 心跳要等到10分钟后开始接受行情才会触发心跳
        now = time.time()
        self.log.info(u'启动汇报')
        self.mainEngine.slavemReport.lanuchReport()

        def foo():
            self.nextHeatBeatTime = now - 1
            self.heartBeat()

        # 10分钟后开始触发一次心跳
        # 避免因为CTP断掉毫无行情，导致心跳从未开始
        Timer(60 * 10, foo).start()

        # 国债期货可以保证 10:15 ~ 10:30 的心跳
        # # 10:15 ~ 10:30 的心跳
        # if arrow.now().datetime.time() < datetime.time(10, 15):
        #     def shock():
        #         self.log.info(u'在休市过程中保持心跳')
        #         while arrow.now().datetime.time() < datetime.time(10, 30):
        #             self.heartBeat()
        #             time.sleep(self.heartBeatInterval)
        #
        #     breakStartTime = arrow.now().replace(hour=10, minute=15)
        #     wait = breakStartTime.timestamp - now
        #     self.log.info(u'设置了 10:15 ~ 10:30 的定时心跳, {} 秒后启动'.format(wait))
        #     Timer(wait, shock).start()

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
        # self.log.info(u'触发心跳')
        self.mainEngine.slavemReport.heartBeat()

    def loadSetting(self):
        super(CtaEngine, self).loadSetting()
        for us in ['ag', 'T']:
            # 订阅 ag 和 T 的主力合约
            sql = {
                'underlyingSymbol': us,
                'activeEndDate': {'$ne': None}
            }
            # 逆序, 取出第一个，就是当前的主力合约
            cursor = self.contractCol.find(sql).sort('activeEndDate', -1)
            d = next(cursor)
            symbol = d['symbol']

            # 订阅合约
            contract = self.mainEngine.getContract(symbol)
            if not contract:
                err = u'找不到维持心跳的合约 {}'.format(symbol)
                self.log.critical(err)
                time.sleep(1)
                raise ValueError(err)

            self.log.info(u'订阅维持心跳的合约 {}'.format(symbol))

            req = VtSubscribeReq()
            req.symbol = contract.symbol
            self.mainEngine.subscribe(req, contract.gatewayName)

            # 仅对 ag 和 T 的tick推送进行心跳
            self.eventEngine.register(EVENT_TICK + symbol, self._heartBeat)

    def sendStopOrder(self, vtSymbol, orderType, price, volume, strategy):
        super(CtaEngine, self).sendStopOrder(vtSymbol, orderType, price, volume, strategy)
        self.log.info(u'{}停止单 {} {} {} {} '.format(vtSymbol, strategy.name, orderType, price, volume))

    def sendOrder(self, vtSymbol, orderType, price, volume, strategy):
        super(CtaEngine, self).sendOrder(vtSymbol, orderType, price, volume, strategy)
        self.log.info(u'{}发单 {} {} {} {} '.format(vtSymbol, strategy.name, orderType, price, volume))

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
        dic = dic.copy()
        dic['datetime'] = arrow.now().datetime
        self.orderBackCol.insert_one(dic)

    def processTradeEvent(self, event):
        super(CtaEngine, self).processTradeEvent(event)

        trade = event.dict_['data']

        # 在完成 strategy.pos 的更新后，保存 trade。trade 也保存更新后的 pos
        if trade.vtOrderID in self.orderStrategyDict:
            self.saveTradeByStrategy(trade)

    def saveTradeByStrategy(self, trade):
        strategy = self.orderStrategyDict[trade.vtOrderID]

        dic = trade.__dict__.copy()
        dic.pop('rawData')

        # 时间戳
        dt = dic['datetime']

        if not dt.tzinfo:
            t = u'成交单 {} {} 没有时区'.format(trade.symbol, dt)
            raise ValueError(t)
        td = dic['tradingDay']
        if td is None:
            t = u'成交单 {} {} 没有交易日'.format(trade.symbol, dt)
            raise ValueError(t)
        dic['class'] = strategy.className
        dic['name'] = strategy.name
        dic['pos'] = strategy.pos

        self.saveTrade(dic)

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

        for s in self.strategyDict.values():
            # 上次检查已经有异常了,这次有异常直接回报
            self._checkPositionByStrategy(s)

    def _checkPositionByStrategy(self, strategy):
        """
        对指定的策略进行仓位检查
        :param strategy:
        :return:
        """
        countDic = self.positionErrorCountDic
        s = strategy
        if not s.trading:
            return

        def errorHandler(err):
            countDic[s] += 1
            if countDic[s] >= self.reportPosErrCount:
                err = u'仓位异常 停止交易 {}'.format(err)
                s.positionErrReport(err)
                s.trading = False
                # 全部撤单
                s.cancelAll()
            else:
                self.log.info(u'仓位出现异常次数 {}'.format(countDic[s]))
                self.log.info(u'{}'.format(err))

        d = self.mainEngine.dataEngine.getPositionDetail(s.vtSymbol)
        assert isinstance(d, PositionDetail)

        if d.longPos != d.longYd + d.longTd:
            # 多头仓位异常
            err = u'{name} longPos:{longPos} longYd:{longYd} longTd:{longTd}'.format(name=s.name, **d.__dict__)
            errorHandler(err)

        elif d.shortPos != d.shortYd + d.shortTd:
            # 空头仓位异常
            err = u'{name} shortPos:{shortPos} shortYd:{shortYd} shortTd:{shortTd}'.format(name=s.name,
                                                                                           **d.__dict__)
            errorHandler(err)

        elif s.pos != d.longPos - d.shortPos:
            err = u'{name} s.pos:{pos} longPos:{longPos} shortPos:{shortPos} '.format(name=s.name, pos=s.pos,
                                                                                      **d.__dict__)
            errorHandler(err)
        else:
            # 没有异常，重置仓位异常次数
            countDic[s] = 0

    def registerEvent(self):
        super(CtaEngine, self).registerEvent()
        self.eventEngine.register(EVENT_TIMER, self.checkPositionDetail)
