#!/bin/bash

cd ..
docker run --rm -it --name vnpyshell \
    -v $PWD:/srv/vnpy \
    -p 8080:8080 \
    vnpy:cta /bin/bash
