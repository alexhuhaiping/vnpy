FROM hub.c.163.com/public/ubuntu:16.04-tools

# 初始化 /etc/machine-id
RUN systemd-machine-id-setup

# 使用传统的 bash 作为 shell 解释器
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# 时区设置
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 中文支持
RUN locale-gen zh_CN.UTF-8 \
    && DEBIAN_FRONTEND=noninteractive dpkg-reconfigure locales \
    && locale-gen zh_CN.UTF-8
ENV LANG zh_CN.UTF-8
ENV LANGUAGE zh_CN:zh
ENV LC_ALL zh_CN.UTF-8

COPY . /srv/vnpy

RUN echo "开始配置系vnpy环境" \
    && echo "更新源列表" \
    && echo "替换 apt 源为阿里云，在本地构建镜像时，取消注释，使用阿里云的 apt 源" \
    && echo "" > /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/ubuntu/ xenial main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/ubuntu/ xenial-backports main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/ubuntu/ xenial-proposed main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/ubuntu/ xenial-security main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/ubuntu/ xenial-updates main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb-src http://mirrors.163.com/ubuntu/ xenial main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb-src http://mirrors.163.com/ubuntu/ xenial-backports main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb-src http://mirrors.163.com/ubuntu/ xenial-proposed main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb-src http://mirrors.163.com/ubuntu/ xenial-security main multiverse restricted universe" >> /etc/apt/sources.list \
    && echo "deb-src http://mirrors.163.com/ubuntu/ xenial-updates main multiverse restricted universe" >> /etc/apt/sources.list \
    && apt-get clean \
    && apt-get update \
    && echo "从 apt 获取软件" \
    && apt-get install -y bzip2 wget git ttf-wqy-microhei \
    && echo "安装编译环境"  \
    && apt-get install -y build-essential libboost-all-dev python-dev cmake \
    && apt-get clean

RUN echo "安装 talib 内核" \
    && pushd /tmp \
    && wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && popd \
    && rm -rf ta-lib*

RUN echo "安装 anaconda" \
    && mkdir /tmp/conda/ \
    && cd /tmp/conda/ \
    && echo "下载 Miniconda by Python3" && wget -t 0 https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && bash Miniconda*.sh -b -p /opt/conda \
    && rm Miniconda*.sh \
    && echo "设置 conda 和 python 的环境路径" \
    && ln -s /opt/conda/bin/pip /usr/local/bin/pip \
    && ln -s /opt/conda/bin/conda /usr/local/bin/conda \
    && rm /usr/bin/python \
    && ln -s /opt/conda/bin/python /usr/bin/python \
    && echo "设置 conda 国内源, 从 conda 安装 python 库" \
    && conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/ \
    && conda config --set show_channel_urls yes \
    && conda install -y pymongo \
    && conda clean -ay

RUN echo "更改 pip 源" \
    && mkdir ~/.pip \
    && echo "[global]" >> ~/.pip/pip.conf \
    && echo "index-url = https://pypi.douban.com/simple" >> ~/.pip/pip.conf \
    && echo "使用 pip 安装 python 库" \
    && echo "必须先安装 numpy 之后再安装 ta-lib " \
    && pip install numpy \
    && pip install ta-lib \
    && locale-gen zh_CN.GB18030

RUN echo "安装 vnpy 项目" \
    && cd /srv/vnpy \
    && pip install -e . \
    && pip install -r requirements.txt \
    && cd ~ \
    && rm -rf /srv/vnpy \
    && rm -rf /var/lib/apt/lists/* \
    && echo "安装结束"

WORKDIR /srv/vnpy
