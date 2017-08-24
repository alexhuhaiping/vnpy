FROM vnpy:origin

RUN cd /srv/vnpy \
    && ls \
    && pip install -r requirements.txt \
    && pip install . \
    && pip install --upgrade \
    && echo "更新镜像结束"
