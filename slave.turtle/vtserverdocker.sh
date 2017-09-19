#!/bin/bash

cd ..
docker run --rm --net host --name vnpy_turtle \
    -v $PWD:/srv/vnpy \
    -v /var/log/svnpy:/var/log/svnpy \
    vnpy:cta /bin/bash
