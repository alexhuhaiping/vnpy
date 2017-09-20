#!/bin/bash


# 尚未调试完成
docker run --rm --net host --name vnpy_turtle \
    -v $PWD:/srv/vnpy \
    -v /var/log/svnpy:/var/log/svnpy \
    vnpy:cta
