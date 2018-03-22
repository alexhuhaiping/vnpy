#!/bin/bash
docker run --rm --name vnpy_turtle \
    -v $PWD:/srv/vnpy \
    -p 38080:38080 \
    vnpy:cta /bin/bash /srv/vnpy/slave.turtle/vtserver.sh