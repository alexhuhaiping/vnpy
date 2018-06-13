# coding:utf-8
from .eventEngine import EventEngine2 as SvtEventEngine2
from .eventEngine import EventEngine, Event
from vnpy.trader.vtFunction import exception


class EventEngine2(SvtEventEngine2):
    pass

    # def register(self, type_, handler):
    #     """注册事件处理函数监听"""
    #     # 尝试获取该事件类型对应的处理函数列表，若无defaultDict会自动创建新的list
    #
    #     handler_ = exception(handler)
    #     return super(EventEngine2, self).register(type_, handler_)
    #
    # def registerGeneralHandler(self, handler):
    #     """注册通用事件处理函数监听"""
    #     handler_ = exception(handler)
    #     return super(EventEngine2, self).registerGeneralHandler(handler_)
