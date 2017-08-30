#!/bin/bash

cd ..
docker run --rm -it --name vnpyshell \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy/vnpy.log:/var/log/svnpy/vnpy.log \
    -p 8080:8080 \
    vnpy:cta /bin/bash
