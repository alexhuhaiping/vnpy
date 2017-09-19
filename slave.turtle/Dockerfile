FROM vnpy:origin

COPY . /srv/vnpy

RUN cd /srv/vnpy \
    && pip install -r requirements.txt \
    && pip install . \
    && pip install --upgrade tradingtime \
    && echo "更新镜像结束"

WORKDIR /srv/vnpy/slave.turtle
CMD [ "./vtserver.sh" ]