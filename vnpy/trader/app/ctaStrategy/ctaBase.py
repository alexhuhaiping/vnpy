# encoding: UTF-8

'''
本文件中包含了CTA模块中用到的一些基础设置、类和常量等。
'''

# CTA引擎中涉及的数据类定义
from vnpy.trader.vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT
from collections import OrderedDict

# 常量定义
# CTA引擎中涉及到的交易方向类型
CTAORDER_BUY = u'买开'
CTAORDER_SELL = u'卖平'
CTAORDER_SHORT = u'卖开'
CTAORDER_COVER = u'买平'

# 本地停止单状态
STOPORDER_WAITING = u'等待中'
STOPORDER_CANCELLED = u'已撤销'
STOPORDER_TRIGGERED = u'已触发'

# 本地停止单前缀
STOPORDERPREFIX = 'CtaStopOrder.'

# 数据库名称
SETTING_DB_NAME = 'VnTrader_Setting_Db'
POSITION_DB_NAME = 'VnTrader_Position_Db'

TICK_DB_NAME = 'VnTrader_Tick_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'

# 引擎类型，用于区分当前策略的运行环境
ENGINETYPE_BACKTESTING = 'backtesting'  # 回测
ENGINETYPE_TRADING = 'trading'          # 实盘

# CTA模块事件
EVENT_CTA_LOG = 'eCtaLog'               # CTA相关的日志事件
EVENT_CTA_STRATEGY = 'eCtaStrategy.'    # CTA策略状态变化事件

# 常量定义
# CTA引擎中涉及到的交易方向类型
CTAORDER_BUY = u'买开'
CTAORDER_SELL = u'卖平'
CTAORDER_SHORT = u'卖开'
CTAORDER_COVER = u'买平'

# 本地停止单状态
STOPORDER_WAITING = u'等待中'
STOPORDER_CANCELLED = u'已撤销'
STOPORDER_TRIGGERED = u'已触发'

# 本地停止单前缀
STOPORDERPREFIX = 'CtaStopOrder.'

# 数据库名称
# SETTING_DB_NAME = 'VnTrader_Setting_Db'
SETTING_DB_NAME = 'cta'
# POSITION_DB_NAME = 'VnTrader_Position_Db'
POSITION_DB_NAME = 'cta'
POSITION_COLLECTION_NAME = 'pos'
TRADE_COLLECTION_NAME = 'trade'
ORDERBACK_COLLECTION_NAME = 'orderback'
CTA_DB_NAME = 'strategy'
CTA_COL_NAME = 'cta'

TICK_DB_NAME = 'VnTrader_Tick_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
# MINUTE_DB_NAME = 'VnTrader_1Min_Db'
MINUTE_DB_NAME = 'ctp'

MINUTE_COL_NAME = 'bar_1min'
DAY_COL_NAME = 'bar_1day'
CONTRACT_COL_NAME = 'contract'

# 引擎类型，用于区分当前策略的运行环境
ENGINETYPE_BACKTESTING = 'backtesting'  # 回测
ENGINETYPE_TRADING = 'trading'          # 实盘

# CTA模块事件
EVENT_CTA_LOG = 'eCtaLog'               # CTA相关的日志事件
EVENT_CTA_STRATEGY = 'eCtaStrategy.'    # CTA策略状态变化事件


# CTA引擎中涉及的数据类定义
from vnpy.trader.vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT


########################################################################
class VtStopOrder(object):
    """本地停止单"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING
        self.orderType = EMPTY_UNICODE
        self.direction = EMPTY_UNICODE
        self.offset = EMPTY_UNICODE
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_INT

        self.strategy = None             # 下停止单的策略对象
        self.stopOrderID = EMPTY_STRING  # 停止单的本地编号 

        self.status = EMPTY_STRING       # 停止单状态


class StopOrder(VtStopOrder):
    def __init__(self):
        super(StopOrder, self).__init__()
        self.unit = None
        self.vtOrderID = None # 触发后，触发后对应的停止单
        self.unit = None # 绑定的对应的 unit
        self.priority = 0 # 同样价格时，成交的优先级。值越小越优先触发

    def __str__(self):
        s = u'< StopOrder '
        s += u'{} '.format(self.stopOrderID)
        s += u'({}) '.format(self.priority)
        s += u'{} '.format(self.offset)
        s += u'{} '.format(self.direction)
        s += u'{} @ {} '.format(self.volume, self.price)
        s += u'{} '.format(self.status)
        s += u'>'
        return s

    def toHtml(self):
        """
        用于网页显示
        :return:
        """

        items = [
            ('stopOrderID', self.stopOrderID),
            ('status', self.status),
            ('orderType', self.orderType),
            ('direction', self.direction),
            ('offset', self.offset),
            ('price', self.price),
            ('volume', self.volume),
            ('priority', self.priority),
        ]

        orderDic = OrderedDict()
        for k, v in items:
            if isinstance(v, float):
                try:
                    # 尝试截掉过长的浮点数
                    v = u'%0.3f' % v
                    while v.endswith('0'):
                        v = v[:-1]
                    if v.endswith('.'):
                        v = v[:-1]
                except:
                    pass
            orderDic[k] = v
        return orderDic