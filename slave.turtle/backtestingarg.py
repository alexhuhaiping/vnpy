# coding:utf-8
import pytz
from bson.codec_options import CodecOptions
from itertools import product
from pymongo import MongoClient
import arrow

# 回测模块的参数
param = {
    'name': u'唐奇安通道',
    'className': 'DonchianChannelStrategy',
    'vtSymbol': None,
    'barPeriod': 10,
    'atrPeriod': 14,
    'unitsNum': 4,
    'hands': 1,
    'maxCD': 1,
    'sys2Vaild': True,
    'capital': 100000,

   # 'group': u'系统2最大CD1~2' + '_' + str(arrow.now().date()),
    'group': u'开发调试',
}
print(u'group: {}'.format(param['group']))

# 要优化的参数，设定优化步长
opts = {
    'barPeriod': [14],
    'maxCD': [1],
}

if not opts:
    raise ValueError(u'未设置需要优化的参数')

param['opts'] = list(opts.values())

# opts = {}

# for k, v in opt.items():
#     b, e, s = v
#     args = [b]
#     while b < e:
#         b += s
#         args.append(b)
#     opts[k] = args

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
strategyArgs = []
for s in settingList:
    d = param.copy()
    d.update(s)
    strategyArgs.append(d)
    d['createTime'] = arrow.now().datetime

mongoKwargs = {
    'host': '192.168.31.208',
    'port': 30020,
}


client = MongoClient(
    **mongoKwargs,
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
    # TODO 测试代码，先只测试螺纹
    if c['underlyingSymbol'] != 'hc':
        continue

    for a in strategyArgs:
        d = a.copy()
        d['vtSymbol'] = c['vtSymbol']
        d['activeStartDate'] = c['activeStartDate']
        d['activeEndDate'] = c['activeEndDate']
        d['priceTick'] = c['priceTick']
        d['size'] = c['size']
        d['underlyingSymbol'] = c['underlyingSymbol']
        documents.append(d)

# 将回测参数保存到数据库
client = MongoClient(
    'localhost',
    30020,
)
username = 'vnpy'
password = 'vnpy'
collName = 'btarg'  # 回测参数
db = client['cta']
db.authenticate(username, password)
coll = db[collName].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('Asia/Shanghai')))

# 删掉同名的参数组
coll.delete_many({'group': d['group'], 'className': d['className']})

coll.insert_many(documents)