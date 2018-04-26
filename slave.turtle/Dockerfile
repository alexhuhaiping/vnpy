FROM vnpy:origin

COPY . /srv/vnpy

RUN cd /srv/vnpy \
    && pip install -r requirements.txt \
    && pip install gevent \
    && pip install -e . \
    && pip install --upgrade tradingtime log4mongo slavem \
    && rm -rf /var/lib/apt/lists/* \
    && echo "更新镜像结束"

WORKDIR /srv/vnpy
