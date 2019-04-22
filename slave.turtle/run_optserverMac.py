# coding:utf-8
import logging.config
loggingConFile = 'loggingMac.conf'
logging.config.fileConfig(loggingConFile)

import optboss

# 读取日志配置文件

# optfile = 'optimizeHome.ini'
optfile = 'optimizeHome.ini'
server = optboss.WorkService(optfile)
server.start()

server.log.info('run 脚本退出')
