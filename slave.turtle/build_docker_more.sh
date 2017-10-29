#/bin/bash

cd ..
docker build -f ./slave.turtle/Dockerfile --force-rm -t vnpy:cta .

# 删除虚悬镜像
docker rmi $(docker images -q -f dangling=true)