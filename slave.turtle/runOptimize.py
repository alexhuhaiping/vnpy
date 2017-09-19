# coding: utf-8

# 优化配置
setting = OptimizationSetting()                 # 新建一个优化任务设置对象
setting.setOptimizeTarget('capital')            # 设置优化排序的目标是策略净盈利
# setting.addParameter('atrLength', 12, 20, 2)    # 增加第一个优化参数atrLength，起始12，结束20，步进2
setting.addParameter('barPeriod', 5, 60, 5)


# 执行多进程优化
engine.runParallelOptimization(DonchianChannelStrategy, setting)