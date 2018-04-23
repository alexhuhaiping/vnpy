#!/bin/bash

./kill_optserver.sh
cd ..

# 运行 Docker 容器
docker run -d --rm --name vnpy_optserver \
    -v $PWD:/srv/vnpy \
    --net host \
    vnpy:cta /bin/bash /srv/vnpy/optization/optserver.sh

sleep 1
docker ps -n 3
