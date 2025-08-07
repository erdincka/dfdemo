FROM maprtech/dev-sandbox-container:7.10.0_9.4.0_ubuntu20

ARG MAPR_REPO=https://package.ezmeral.hpe.com/releases/
ARG UBUNTU_VERSION=22.04
ARG NIFI_VERSION=1.28.0
ARG MYSQL_CONNECTOR_VERSION=9.3.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y git python3-dev gcc tree locales
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

ENV MAPR_HOME=/opt/mapr

WORKDIR /app
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j_$MYSQL_CONNECTOR_VERSION-1ubuntu"${UBUNTU_VERSION}"_all.deb
RUN dpkg -i ./mysql-connector-j_$MYSQL_CONNECTOR_VERSION-1ubuntu"${UBUNTU_VERSION}"_all.deb

# Replace dev repo
RUN sed -i "s|http://dfaf.mip.storage.hpecorp.net/artifactory/list/prestage/releases|${MAPR_REPO}|g" /etc/apt/sources.list.d/mapr.list
RUN apt update && apt upgrade -y && apt install -y \
    mapr-nifi \
    mapr-drill \
    mapr-grafana \
    mapr-spark \
    mapr-spark-thriftserver \
    mysql-client

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

EXPOSE 8443 8501 2222 8047 8780 12443 3000 8780

# fix init-script
RUN sed -i '/after cldb /a     sleep 30; echo mapr | maprlogin password -user mapr' /usr/bin/init-script

# Add CDC support to NiFi
RUN wget -P /opt/mapr/nifi/nifi-"${NIFI_VERSION}"/extensions https://repo1.maven.org/maven2/org/apache/nifi/nifi-cdc-mysql-nar/"${NIFI_VERSION}"/nifi-cdc-mysql-nar-"${NIFI_VERSION}".nar

COPY entrypoint.sh /

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN echo "export UV_ENV_FILE=.env" >> $HOME/.profile
RUN echo "export LD_LIBRARY_PATH=/opt/mapr/lib" >> $HOME/.profile
ENV CFLAGS=-I/opt/mapr/include
ENV LDFLAGS=-L/opt/mapr/lib

# COPY APP FILES
# COPY app/pyproject.toml app/uv.lock app/.python-version /app/
COPY app/* /app/
RUN /root/.local/bin/uv sync

# Enable Iceberg for Spark
RUN wget -P /opt/mapr/spark/spark-3.5.5/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

COPY mysql.cnf /etc/mysql/conf.d/client.cnf

# Cleanup
RUN rm ./mysql-connector-j_"${MYSQL_CONNECTOR_VERSION}"-1ubuntu"${UBUNTU_VERSION}"_all.deb
