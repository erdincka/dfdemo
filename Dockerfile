FROM maprtech/dev-sandbox-container:7.10.0_9.4.0_ubuntu20

ARG MAPR_REPO=https://package.ezmeral.hpe.com/releases/
ARG UBUNTU_VERSION=22.04
ARG NIFI_VERSION=1.28.0
ARG MYSQL_CONNECTOR_VERSION=9.3.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y git python3-dev gcc tree locales
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8

ENV MAPR_HOME=/opt/mapr

# fix init-script
RUN sed -i '/after cldb /a     sleep 30; echo mapr | maprlogin password -user mapr' /usr/bin/init-script


COPY . /app
WORKDIR /app
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j_$MYSQL_CONNECTOR_VERSION-1ubuntu"${UBUNTU_VERSION}"_all.deb
RUN dpkg -i ./mysql-connector-j_$MYSQL_CONNECTOR_VERSION-1ubuntu"${UBUNTU_VERSION}"_all.deb

# Replace dev repo
RUN sed -i "s|http://dfaf.mip.storage.hpecorp.net/artifactory/list/prestage/releases|${MAPR_REPO}|g" /etc/apt/sources.list.d/mapr.list
RUN apt update && apt upgrade -y && apt install -y \
    mapr-drill \
    mysql-server

# mapr-nifi \
# mapr-spark \
# mapr-spark-thriftserver \
# mapr-grafana \
EXPOSE 9443 8443 8501 2222 8047

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN echo "export UV_ENV_FILE=.env" >> $HOME/.profile
RUN echo "export LD_LIBRARY_PATH=/opt/mapr/lib" >> $HOME/.profile
ENV CFLAGS=-I/opt/mapr/include
ENV LDFLAGS=-L/opt/mapr/lib
RUN /root/.local/bin/uv sync

RUN . $HOME/.local/bin/env && uv add mapr-streams-python

# Cleanup
RUN rm ./mysql-connector-j_"${MYSQL_CONNECTOR_VERSION}"-1ubuntu"${UBUNTU_VERSION}"_all.deb
