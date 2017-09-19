#/bin/bash

docker rmi vnpy:cta
docker build --force-rm -t vnpy:cta .

# 删除虚悬镜像
docker rmi $(docker images -q -f dangling=true)