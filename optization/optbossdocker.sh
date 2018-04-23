#!/bin/bash

./kill_optboss.sh
cd ..

# 运行 Docker 容器
docker run -d --rm --name vnpy_optboss \
    -v $PWD:/srv/vnpy \
    --net host \
    vnpy:cta /bin/bash /srv/vnpy/optization/optboss.sh

sleep 1
docker ps -n 3
