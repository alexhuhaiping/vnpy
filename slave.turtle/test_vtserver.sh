#!/bin/bash

./kill_turtle.sh
cd ..
docker run --rm --name vnpy_turtle \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    -p 8080:8080 \
    vnpy:cta /usr/local/bin/python runCtaTrading.py

docker ps -n 3
