#!/bin/bash

./kill_opt.sh
cd ..
docker run --rm --name vnpy_optserver \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    -p 30050:30050 \
    vnpy:cta /bin/bash /srv/vnpy/optization/optserver.sh

docker ps -n 3
