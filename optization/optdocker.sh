#!/bin/bash

./kill_opt.sh
cd ..
docker run --rm --name vnpy_opt \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    vnpy:cta /bin/bash /srv/vnpy/optization/opt.sh

docker ps -n 3
