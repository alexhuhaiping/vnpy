FROM vnpy:origin

COPY . /srv/vnpy

RUN cd /srv/vnpy \
    && pip install -r requirements.txt \
    && pip install -e . \
    && pip install --upgrade tradingtime==2017.11.20 log4mongo \
    && echo "更新镜像结束"

WORKDIR /srv/vnpy
