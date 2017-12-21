# encoding: UTF-8

import datetime as dtt

import arrow
import tradingtime as tt

from vnpy.event import *
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import *
from vnpy.trader.vtFunction import LOCAL_TIMEZONE


########################################################################
class sVtGateway(object):
    """交易接口"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName):
        """Constructor"""
        self.eventEngine = eventEngine
        self.gatewayName = gatewayName
        
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """市场行情推送"""
        # 通用事件
        event1 = Event(type_=EVENT_TICK)
        event1.dict_['data'] = tick
        self.eventEngine.put(event1)
        
        # 特定合约代码的事件
        event2 = Event(type_=EVENT_TICK+tick.vtSymbol)
        event2.dict_['data'] = tick
        self.eventEngine.put(event2)
    
    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """成交信息推送"""
        # 通用事件
        event1 = Event(type_=EVENT_TRADE)
        event1.dict_['data'] = trade
        self.eventEngine.put(event1)
        
        # 特定合约的成交事件
        event2 = Event(type_=EVENT_TRADE+trade.vtSymbol)
        event2.dict_['data'] = trade
        self.eventEngine.put(event2)        
    
    #----------------------------------------------------------------------
    def onOrder(self, order):
        """订单变化推送"""
        # 通用事件
        event1 = Event(type_=EVENT_ORDER)
        event1.dict_['data'] = order
        self.eventEngine.put(event1)
        
        # 特定订单编号的事件
        event2 = Event(type_=EVENT_ORDER+order.vtOrderID)
        event2.dict_['data'] = order
        self.eventEngine.put(event2)
    
    #----------------------------------------------------------------------
    def onPosition(self, position):
        """持仓信息推送"""
        # 通用事件
        event1 = Event(type_=EVENT_POSITION)
        event1.dict_['data'] = position
        self.eventEngine.put(event1)
        
        # 特定合约代码的事件
        event2 = Event(type_=EVENT_POSITION+position.vtSymbol)
        event2.dict_['data'] = position
        self.eventEngine.put(event2)
    
    #----------------------------------------------------------------------
    def onAccount(self, account):
        """账户信息推送"""
        # 通用事件
        event1 = Event(type_=EVENT_ACCOUNT)
        event1.dict_['data'] = account
        self.eventEngine.put(event1)
        
        # 特定合约代码的事件
        event2 = Event(type_=EVENT_ACCOUNT+account.vtAccountID)
        event2.dict_['data'] = account
        self.eventEngine.put(event2)
    
    #----------------------------------------------------------------------
    def onError(self, error):
        """错误信息推送"""
        # 通用事件
        event1 = Event(type_=EVENT_ERROR)
        event1.dict_['data'] = error
        self.eventEngine.put(event1)    
        
    #----------------------------------------------------------------------
    def onLog(self, log):
        """日志推送"""
        # 通用事件
        event1 = Event(type_=EVENT_LOG)
        event1.dict_['data'] = log
        self.eventEngine.put(event1)
        
    #----------------------------------------------------------------------
    def onContract(self, contract):
        """合约基础信息推送"""
        # 通用事件
        event1 = Event(type_=EVENT_CONTRACT)
        event1.dict_['data'] = contract
        self.eventEngine.put(event1)

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        pass
    
    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        pass
    
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        pass
    
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        pass
    
    #----------------------------------------------------------------------
    def qryAccount(self):
        """查询账户资金"""
        pass
    
    #----------------------------------------------------------------------
    def qryPosition(self):
        """查询持仓"""
        pass
    
    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        pass
    

class VtGateway(sVtGateway):
    def onMraginRate(self, marginRate):
        """
        保证金率推送
        :param marginRate:
        :return:
        """
        event1 = Event(type_=EVENT_MARGIN_RATE)
        event1.dict_['data'] = marginRate
        self.eventEngine.put(event1)

    def onCommissionRate(self, commisionRate):
        """
        手续费率推送
        :param commisionRate:
        :return:
        """
        assert isinstance(commisionRate, VtCommissionRate)
        event1 = Event(type_=EVENT_COMMISSION_RATE)
        event1.dict_['data'] = commisionRate

        self.eventEngine.put(event1)


    def onTrade(self, trade):
        """
        推送前增加时间戳和交易日属性
        :param trade:
        :return:
        """
        now = arrow.now().datetime
        t = dtt.time(*list(map(int, trade.tradeTime.split(':'))))
        tradeTime = dtt.datetime.combine(dtt.date.today(), t)
        tradeTime = LOCAL_TIMEZONE.localize(tradeTime)
        if now - tradeTime < dtt.timedelta(hours=1):
            # 回报和本地时间差在1个小时内则没有跨日
            pass
        else:
            # 跨日了
            tradeTime = dtt.datetime.combine(dtt.date.today() - dtt.timedelta(days=1), t)
            tradeTime = LOCAL_TIMEZONE.localize(tradeTime)

        _, tradingDay = tt.get_tradingday(tradeTime)

        trade.datetime = tradeTime
        trade.tradingDay = tradingDay

        super(VtGateway, self).onTrade(trade)
