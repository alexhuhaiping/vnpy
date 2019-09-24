# encoding: UTF-8

import time
from logging import INFO

from vnpy.trader.vtConstant import (EMPTY_STRING, EMPTY_UNICODE,
                                    EMPTY_FLOAT, EMPTY_INT, DIRECTION_LONG, DIRECTION_SHORT)


########################################################################
class VtBaseData(object):
    """回调函数推送数据的基础类，其他数据类继承于此"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.gatewayName = EMPTY_STRING  # Gateway名称
        self.rawData = None  # 原始数据


########################################################################
class VtTickData(VtBaseData):
    """Tick行情数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtTickData, self).__init__()

        # 代码相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        # 成交数据
        self.lastPrice = EMPTY_FLOAT  # 最新成交价
        self.lastVolume = EMPTY_INT  # 最新成交量
        self.volume = EMPTY_INT  # 今天总成交量
        self.openInterest = EMPTY_INT  # 持仓量
        self.time = EMPTY_STRING  # 时间 11:20:56.5
        self.date = EMPTY_STRING  # 日期 20151009
        self.datetime = None  # python的datetime时间对象

        # 常规行情
        self.openPrice = EMPTY_FLOAT  # 今日开盘价
        self.highPrice = EMPTY_FLOAT  # 今日最高价
        self.lowPrice = EMPTY_FLOAT  # 今日最低价
        self.preClosePrice = EMPTY_FLOAT

        self.upperLimit = EMPTY_FLOAT  # 涨停价
        self.lowerLimit = EMPTY_FLOAT  # 跌停价

        # 五档行情
        self.bidPrice1 = EMPTY_FLOAT
        self.bidPrice2 = EMPTY_FLOAT
        self.bidPrice3 = EMPTY_FLOAT
        self.bidPrice4 = EMPTY_FLOAT
        self.bidPrice5 = EMPTY_FLOAT

        self.askPrice1 = EMPTY_FLOAT
        self.askPrice2 = EMPTY_FLOAT
        self.askPrice3 = EMPTY_FLOAT
        self.askPrice4 = EMPTY_FLOAT
        self.askPrice5 = EMPTY_FLOAT

        self.bidVolume1 = EMPTY_INT
        self.bidVolume2 = EMPTY_INT
        self.bidVolume3 = EMPTY_INT
        self.bidVolume4 = EMPTY_INT
        self.bidVolume5 = EMPTY_INT

        self.askVolume1 = EMPTY_INT
        self.askVolume2 = EMPTY_INT
        self.askVolume3 = EMPTY_INT
        self.askVolume4 = EMPTY_INT
        self.askVolume5 = EMPTY_INT

    ########################################################################


class VtBarData(VtBaseData):
    """K线数据"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtBarData, self).__init__()

        self.vtSymbol = EMPTY_STRING  # vt系统代码
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所

        self.open = EMPTY_FLOAT  # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT

        self.date = EMPTY_STRING  # bar开始的时间，日期
        self.time = EMPTY_STRING  # 时间
        self.datetime = None  # python的datetime时间对象
        self.tradingDay = None  # python的datetime时间对象

        self.volume = EMPTY_INT  # 成交量
        self.openInterest = EMPTY_INT  # 持仓量

    def load(self, dic):
        """

        :param dic:
        :return:
        """
        self.rawData = dic
        for k, v in list(dic.items()):
            setattr(self, k, v)
        self.vtSymbol = self.symbol

    def dump(self):
        """

        :return:
        """
        return self.rawData

    def __str__(self):
        return 'd {} t {} o {} h {} l {} c {}'.format(self.datetime, self.tradingDay, self.open, self.high, self.low, self.close)


########################################################################
class VtTradeData(VtBaseData):
    """成交数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtTradeData, self).__init__()

        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.tradeID = EMPTY_STRING  # 成交编号
        self.vtTradeID = EMPTY_STRING  # 成交在vt系统中的唯一编号，通常是 Gateway名.成交编号

        self.orderID = EMPTY_STRING  # 订单编号
        self.vtOrderID = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

        # 成交相关
        self.direction = EMPTY_UNICODE  # 成交方向
        self.offset = EMPTY_UNICODE  # 成交开平仓
        self.price = EMPTY_FLOAT  # 成交价格
        self.volume = EMPTY_INT  # 成交数量
        self.tradeTime = EMPTY_STRING  # 成交时间
        self.tradingDay = None # 交易日
        self.datetime = None # 成交时间戳

        self.stopPrice = None  # 停止单的价格

    @property
    def splippage(self):
        """
        负数为亏损的滑点，正数为盈利的滑点
        :return:
        """
        try:
            splippage = self.price - self.stopPrice
            return - splippage if self.direction == DIRECTION_LONG else splippage
        except TypeError:
            return None


########################################################################
class VtOrderData(VtBaseData):
    """订单数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtOrderData, self).__init__()

        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.orderID = EMPTY_STRING  # 订单编号
        self.vtOrderID = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

        # 报单相关
        self.direction = EMPTY_UNICODE  # 报单方向
        self.offset = EMPTY_UNICODE  # 报单开平仓
        self.price = EMPTY_FLOAT  # 报单价格
        self.totalVolume = EMPTY_INT  # 报单总数量
        self.tradedVolume = EMPTY_INT  # 报单成交数量
        self.status = EMPTY_UNICODE  # 报单状态

        self.orderTime = EMPTY_STRING  # 发单时间
        self.cancelTime = EMPTY_STRING  # 撤单时间

        # CTP/LTS相关
        self.frontID = EMPTY_INT  # 前置机编号
        self.sessionID = EMPTY_INT  # 连接编号


########################################################################
class VtPositionData(VtBaseData):
    """持仓数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtPositionData, self).__init__()

        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，合约代码.交易所代码

        # 持仓相关
        self.direction = EMPTY_STRING  # 持仓方向
        self.position = EMPTY_INT  # 持仓量
        self.frozen = EMPTY_INT  # 冻结数量
        self.price = EMPTY_FLOAT  # 持仓均价
        self.vtPositionName = EMPTY_STRING  # 持仓在vt系统中的唯一代码，通常是vtSymbol.方向
        self.ydPosition = EMPTY_INT  # 昨持仓
        self.positionProfit = EMPTY_FLOAT  # 持仓盈亏


########################################################################
class VtAccountData(VtBaseData):
    """账户数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtAccountData, self).__init__()

        # 账号代码相关
        self.accountID = EMPTY_STRING  # 账户代码
        self.vtAccountID = EMPTY_STRING  # 账户在vt中的唯一代码，通常是 Gateway名.账户代码

        # 数值相关
        self.preBalance = EMPTY_FLOAT  # 昨日账户结算净值
        self.balance = EMPTY_FLOAT  # 账户净值
        self.available = EMPTY_FLOAT  # 可用资金
        self.commission = EMPTY_FLOAT  # 今日手续费
        self.margin = EMPTY_FLOAT  # 保证金占用
        self.closeProfit = EMPTY_FLOAT  # 平仓盈亏
        self.positionProfit = EMPTY_FLOAT  # 持仓盈亏


########################################################################
class VtErrorData(VtBaseData):
    """错误数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtErrorData, self).__init__()

        self.errorID = EMPTY_STRING  # 错误代码
        self.errorMsg = EMPTY_UNICODE  # 错误信息
        self.additionalInfo = EMPTY_UNICODE  # 补充信息

        self.errorTime = time.strftime('%X', time.localtime())  # 错误生成时间


########################################################################
class VtLogData(VtBaseData):
    """日志数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtLogData, self).__init__()

        self.logTime = time.strftime('%X', time.localtime())    # 日志生成时间
        self.logContent = EMPTY_UNICODE                         # 日志信息
        self.logLevel = INFO                                    # 日志级别


########################################################################
class VtContractData(VtBaseData):
    """合约详细信息类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtContractData, self).__init__()

        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
        self.name = EMPTY_UNICODE  # 合约中文名

        self.productClass = EMPTY_UNICODE  # 合约类型
        self.size = EMPTY_INT  # 合约大小
        self.priceTick = EMPTY_FLOAT  # 合约最小价格TICK

        # 期权相关
        self.strikePrice = EMPTY_FLOAT  # 期权行权价
        self.underlyingSymbol = EMPTY_STRING  # 标的物合约代码
        self.optionType = EMPTY_UNICODE  # 期权类型
        self.expiryDate = EMPTY_STRING  # 到期日

        # 原始数据
        self.InstrumentID = ''
        self.ExchangeID = ''
        self.InstrumentName = ''
        self.ExchangeInstID = ''
        self.ProductID = ''
        self.ProductClass = ''
        self.DeliveryYear = 0
        self.DeliveryMonth = 0
        self.MaxMarketOrderVolume = 0
        self.MinMarketOrderVolume = 0
        self.MaxLimitOrderVolume = 0
        self.MinLimitOrderVolume = 0
        self.VolumeMultiple = 0
        self.PriceTick = 0.
        self.CreateDate = ''
        self.OpenDate = ''
        self.ExpireDate = ''
        self.StartDelivDate = ''
        self.EndDelivDate = ''
        self.InstLifePhase = ''
        self.IsTrading = 0
        self.PositionType = ''
        self.PositionDateType = ''
        self.LongMarginRatio = 0.
        self.ShortMarginRatio = 0.
        self.MaxMarginSideAlgorithm = ''
        self.UnderlyingInstrID = ''
        self.StrikePrice = 0.
        self.OptionsType = ''
        self.UnderlyingMultiple = 0.
        self.CombinationType = ''


    def toFuturesDB(self):
        """

        :return:
        """
        dic = self.__dict__.copy()
        dic.pop('rawData')
        return dic

    def fromRawData(self):
        """
        InstrumentID rb1910 <class 'str'>
        ExchangeID SHFE <class 'str'>
        InstrumentName rb1910 <class 'str'>
        ExchangeInstID rb1910 <class 'str'>
        ProductID rb <class 'str'>
        ProductClass 1 <class 'str'>
        DeliveryYear 2019 <class 'int'>
        DeliveryMonth 10 <class 'int'>
        MaxMarketOrderVolume 30 <class 'int'>
        MinMarketOrderVolume 1 <class 'int'>
        MaxLimitOrderVolume 500 <class 'int'>
        MinLimitOrderVolume 1 <class 'int'>
        VolumeMultiple 10 <class 'int'>
        PriceTick 1.0 <class 'float'>
        CreateDate 20180912 <class 'str'>
        OpenDate 20181016 <class 'str'>
        ExpireDate 20191015 <class 'str'>
        StartDelivDate 20191016 <class 'str'>
        EndDelivDate 20191022 <class 'str'>
        InstLifePhase 1 <class 'str'>
        IsTrading 1 <class 'int'>
        PositionType 2 <class 'str'>
        PositionDateType 1 <class 'str'>
        LongMarginRatio 0.08 <class 'float'>
        ShortMarginRatio 0.08 <class 'float'>
        MaxMarginSideAlgorithm 1 <class 'str'>
        UnderlyingInstrID rb <class 'str'>
        StrikePrice 0.0 <class 'float'>
        OptionsType 0 <class 'str'>
        UnderlyingMultiple 1.0 <class 'float'>
        CombinationType 0 <class 'str'>
        :return:
        """
        for k,v in self.rawData.items():
            setattr(self, k, v)

        # 生成 vtSymbol 的规则
        self.vtSymbol = self.toVtSymbol(**self.rawData)

    @staticmethod
    def toVtSymbol(ProductID, DeliveryYear, DeliveryMonth, ExchangeID, **kwargs):
        return f'{ProductID}{str(DeliveryYear)[-2:]}{str(DeliveryMonth).zfill(2)}.{ExchangeID}'

########################################################################
class VtMarginRate(VtBaseData):
    """保证金率类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtMarginRate, self).__init__()

        self.vtSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
        self.ShortMarginRatioByMoney = EMPTY_FLOAT  # 该合约的保证金率
        self.LongMarginRatioByMoney = EMPTY_FLOAT  # 该合约的保证金率
        self.marginRate = 1.

    def loadFromContract(self, dic):
        for k in list(self.__dict__.keys()):
            if k in dic:
                setattr(self, k, dic[k])


########################################################################
class VtCommissionRate(VtBaseData):
    """手续费"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtCommissionRate, self).__init__()

        self.underlyingSymbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
        self.investorRange = EMPTY_STRING

        self.openRatioByMoney = EMPTY_FLOAT
        self.closeRatioByMoney = EMPTY_FLOAT
        self.closeTodayRatioByMoney = EMPTY_FLOAT

        self.openRatioByVolume = EMPTY_FLOAT
        self.closeRatioByVolume = EMPTY_FLOAT
        self.closeTodayRatioByVolume = EMPTY_FLOAT

    def loadFromContract(self, dic):
        for k in list(self.__dict__.keys()):
            if k in dic:
                setattr(self, k, dic[k])


########################################################################
class VtSubscribeReq(object):
    """订阅行情时传入的对象类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所

        # 以下为IB相关
        self.productClass = EMPTY_UNICODE  # 合约类型
        self.currency = EMPTY_STRING  # 合约货币
        self.expiry = EMPTY_STRING  # 到期日
        self.strikePrice = EMPTY_FLOAT  # 行权价
        self.optionType = EMPTY_UNICODE  # 期权类型


########################################################################
class VtOrderReq(object):
    """发单时传入的对象类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所
        self.vtSymbol = EMPTY_STRING            # VT合约代码
        self.price = EMPTY_FLOAT                # 价格
        self.volume = EMPTY_INT                 # 数量

        self.priceType = EMPTY_STRING           # 价格类型
        self.direction = EMPTY_STRING           # 买卖
        self.offset = EMPTY_STRING              # 开平

        # 以下为IB相关
        self.productClass = EMPTY_UNICODE  # 合约类型
        self.currency = EMPTY_STRING  # 合约货币
        self.expiry = EMPTY_STRING  # 到期日
        self.strikePrice = EMPTY_FLOAT  # 行权价
        self.optionType = EMPTY_UNICODE  # 期权类型
        self.lastTradeDateOrContractMonth = EMPTY_STRING  # 合约月,IB专用
        self.multiplier = EMPTY_STRING  # 乘数,IB专用


########################################################################
class VtCancelOrderReq(object):
    """撤单时传入的对象类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所
        self.vtSymbol = EMPTY_STRING            # VT合约代码
        self.vtOrderID = EMPTY_STRING           # VT
        
        # 以下字段主要和CTP、LTS类接口相关
        self.orderID = EMPTY_STRING  # 报单号
        self.frontID = EMPTY_STRING  # 前置机号
        self.sessionID = EMPTY_STRING  # 会话号
