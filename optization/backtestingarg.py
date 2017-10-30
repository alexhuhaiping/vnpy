# coding:utf-8

import json
from collections import OrderedDict
import pytz
from bson.codec_options import CodecOptions
from itertools import product
from pymongo import MongoClient
import arrow
import ConfigParser

from vnpy.trader.vtFunction import getTempPath, getJsonPath

config = ConfigParser.SafeConfigParser()
configPath = getJsonPath('optimize.ini', __file__)

with open(configPath, 'r') as f:
    config.readfp(f)

fileName = 'opt_CCI_SvtBollChannel.json'

with open(fileName, 'r') as f:
    dic = json.load(f)

param = dic['param']
opts = OrderedDict(dic['opts'])

print(u'group: {}'.format(param['group']))

# 回测模块的参数

if not opts:
    raise ValueError(u'未设置需要优化的参数')

param['opts'] = list(opts.keys())

"""生成优化参数组合"""
# 参数名的列表
nameList = opts.keys()
paramList = opts.values()

# 使用迭代工具生产参数对组合
productList = list(product(*paramList))

# 把参数对组合打包到一个个字典组成的列表中
settingList = []
for p in productList:
    d = dict(zip(nameList, p))
    settingList.append(d)

# 策略参数组合
keyList = list(opts.keys())
keyList.sort()
strategyArgs = []
for s in settingList:
    d = param.copy()
    d.update(s)
    # 将待优化的参数组合成唯一索引
    d['optsv'] = ','.join(['{}:{}'.format(n, d[n]) for n in keyList])
    strategyArgs.append(d)
    d['createTime'] = arrow.now().datetime

mongoKwargs = {
    'host': '192.168.31.208',
    'port': 30020,
}

client = MongoClient(
    **mongoKwargs
)

# 读取合约信息
username = 'vnpy'
password = 'a90asdl22cv0SjS2dac'

db = client['ctp']
db.authenticate(username, password)

coll = db['contract'].with_options(
    codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

sql = {
    'activeStartDate': {'$ne': None},
    'activeEndDate': {'$ne': None}
}
cursor = coll.find(sql)
cursor.sort('activeEndDate', -1)

# 每个品种的回测参数
documents = []
for c in cursor:
    # # TODO 测试代码，先只测试螺纹
    if c['underlyingSymbol'] != 'hc':
        # if c['vtSymbol'] != 'hc1710':
        continue

    for a in strategyArgs:
        d = a.copy()
        d['optsv'] = '{},{}'.format(c['underlyingSymbol'], d['optsv'])
        d['vtSymbol'] = c['vtSymbol']
        d['activeStartDate'] = c['activeStartDate']
        d['activeEndDate'] = c['activeEndDate']
        d['priceTick'] = c['priceTick']
        d['size'] = c['size']
        d['underlyingSymbol'] = c['underlyingSymbol']
        documents.append(d)

count = len(documents)
if count > 1000:
    countStr = u'{}万'.format(count/ 10000.)
else:
    countStr = count
print(u'生成 {} 组参数'.format(countStr))

# 将回测参数保存到数据库
host = config.get('mongo', 'host')
port = config.getint('mongo', 'port')
username = config.get('mongo', 'username')
password = config.get('mongo', 'password')
dbn = config.get('mongo', 'dbn')

print(u'参数存入 {}:{}/{}'.format(host, port, dbn))

client = MongoClient(
    host,
    port,
)

collName = 'btarg'  # 回测参数
db = client[dbn]
db.authenticate(username, password)
coll = db[collName].with_options(
    codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

# 删掉同名的参数组
coll.delete_many({'group': d['group'], 'className': d['className']})
coll.insert_many(documents)
