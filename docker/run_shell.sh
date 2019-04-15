#!/usr/bin/env bash
# 以 shell 方式启动容器，环境如同 Linux 命令行

docker rm -f vnpy_shell
cd ..
# 运行 Docker 容器
docker run --rm --name vnpy_shell \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    -it vnpy /bin/bash