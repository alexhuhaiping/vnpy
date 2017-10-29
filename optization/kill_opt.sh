#!/bin/bash

# 发出终止信号，触发退出操作
docker kill -s SIGTERM vnpy_opt && echo '发出终止信号'
# 等待操作完成
sleep 2
# 强制关闭容器
docker rm -f vnpy_opt && echo '关闭容器'
# 关闭网络路由
docker network disconnect --force host vnpy_opt
