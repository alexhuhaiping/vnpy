#!/bin/bash

docker rm -f vnpyshell
cd ..
docker run --rm -it --name vnpyshell \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    -p 8080:8080 \
    vnpy:cta /bin/bash
