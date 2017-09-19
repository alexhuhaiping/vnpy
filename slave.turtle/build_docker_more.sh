#/bin/bash

cd ..
docker rmi vnpy:cta
docker -f ./savle.turtle/Dockerfile build --force-rm -t vnpy:cta .

# 删除虚悬镜像
docker rmi $(docker images -q -f dangling=true)