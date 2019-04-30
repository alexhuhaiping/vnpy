FROM vnpy:1.7

COPY . /srv/vnpy

RUN echo "更新镜像" \
    && cd ~ \
    && pip install gevent \
    && rm -rf /srv/vnpy \
    && rm -rf /var/lib/apt/lists/* \
    && echo "更新镜像结束"

WORKDIR /srv/vnpy/slave.turtle
