from itertools import product
from pymongo import MongoClient
import arrow

# 回测模块的参数
param = {
    'name': '唐奇安通道',
    'className': 'DonchianChannelStrategy',
    'vtSymbol': None,
    'barPeriod': 10,
    'atrPeriod': 14,
    'barMinute': None,
    'unitsNum': 4,
    'hands': 1,
    'maxCD': 1,
    'sys2Vaild': True,

    'group': '系统2最大CD1~3' + '_' + str(arrow.now().date()),
}

# 要优化的参数，设定优化步长
opts = {
    'barPeriod': [5, 7, 9, 12, 14, 16, 18, 21, 24, 27, 31, 43, 60],
    'maxCD': [1, 2],
}

if not opts:
    raise ValueError('未设置需要优化的参数')

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

if __debug__:
    mongoKwargs = {
        'host': 'localhost',
        'port': 30020,
    }

client = MongoClient(
    **mongoKwargs,
)

# 读取合约信息
username = 'vnpy'
password = 'a90asdl22cv0SjS2dac'
if __debug__:
    password = 'vnpy'
db = client['ctp']
db.authenticate(username, password)
coll = db['contract']
sql = {
    'activeStartDate': {'$ne': None},
    'activeEndDate': {'$ne': None}
}
cursor = coll.find(sql)
cursor.sort('activeEndDate', -1)

# 每个品种的回测参数
documents = []
for c in cursor:
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
username = 'vnpy'
password = 'vnpy'
collName = 'argturtle'
db = client['backtesting']
db.authenticate(username, password)
coll = db[collName]

# 删掉同名的参数组
coll.delete_many({'group': d['group'], 'className': d['className']})

coll.insert_many(documents)
