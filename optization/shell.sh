#!/bin/bash

#!/bin/bash

docker rm -f vnpyshell
cd ..
docker run --rm -it --name vnpyshell \
    -v $PWD:/srv/vnpy \
    -v /private/var/log/svnpy:/var/log/svnpy \
    -p 30050:30050 \
    vnpy:cta /bin/bash
