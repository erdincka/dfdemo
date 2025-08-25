#!/usr/bin/env bash

# PACC configuation
sed -i 's/^deb/#deb/g' /etc/apt/sources.list.d/mapr*

apt -qq update && apt -qq install -y locales netbase sshpass git python3-pip
# apt install -y mapr-spark # no spark functions in use.
export SHELL=/bin/bash \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8
locale-gen $LC_ALL

# Enable streams and db clients
pip3 install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" mapr-streams-python
pip3 install maprdb-python-client
export LD_LIBRARY_PATH=/opt/mapr/lib
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
[ -d /mapr ] || mkdir /mapr

# App configuration
[ -d /app ] || git clone --depth=1 https://github.com/erdincka/catchx.git /app

pip install nicegui==1.4.37 requests importlib_resources \
    faker pyiceberg[hive,pandas,s3fs] deltalake sqlalchemy \
    country_converter pycountry PyMySQL minio # geopy
# override versions installed by others
pip install protobuf==3.20.*

cd /app
LD_LIBRARY_PATH=/opt/mapr/lib python3 main.py

# don't exit when service dies.
sleep infinity
