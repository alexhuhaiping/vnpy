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

import traceback
import arrow
import datetime
from itertools import chain
from bson.codec_options import CodecOptions

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTickData, VtBarData
from vnpy.trader.vtGateway import VtSubscribeReq, VtOrderReq, VtCancelOrderReq, VtLogData
from vnpy.trader.vtFunction import todayDate, getJsonPath
from vnpy.trader.app.ctaStrategy.ctaEngine import CtaEngine as VtCtaEngine

from .ctaBase import *
from .strategy import STRATEGY_CLASS


########################################################################
class CtaEngine(VtCtaEngine):
    """CTA策略引擎"""

    @property
    def LOCAL_TIMEZONE(self):
        return self.mainEngine.LOCAL_TIMEZONE

    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)

        # 历史行情的 collection
        self.mainEngine.dbConnect()

        # 1min bar
        self.ctpCol1minBar = self.mainEngine.ctpdb['bar_1min'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.LOCAL_TIMEZONE))

        # 日线 bar
        self.ctpCol1dayBar = self.mainEngine.ctpdb['bar_1day'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.LOCAL_TIMEZONE))

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

        # 总的需要载入的 bar 数量
        barAmount = barNum * barPeriod

        loadDate = self.today
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
                noDataDays += 1
                doc = [i for i in cursor]
                doc.sort(key=lambda bar: bar['datetime'])
                documents.append(doc)
                loadBarNum += cursor.count()
                if loadBarNum > barAmount:
                    # 数量够了， 跳出循环
                    break
            else:
                # 没有任何数据
                noDataDays = 0
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
            if params:
                func(params)
            else:
                func()
        except Exception:
            # 停止策略，修改状态为未初始化
            strategy.trading = False
            strategy.inited = False

            # 发出日志
            content = '\n'.join([u'策略%s触发异常已停止' % strategy.name,
                                 traceback.format_exc()])
            self.log.error(content)
