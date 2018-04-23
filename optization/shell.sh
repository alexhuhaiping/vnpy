#!/bin/bash

#!/bin/bash

docker rm -f vnpyshell
cd ..
docker run --rm -it --name vnpyshell \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/opt:/var/log/opt \
    --net host \
    vnpy:cta /bin/bash
