FROM vnpy:1.7

COPY . /srv/vnpy

RUN echo "更新镜像" \
    && cd ~ \
    && rm -rf /srv/vnpy \
    && echo "更新镜像结束"

WORKDIR /srv/vnpy/slave.turtle
