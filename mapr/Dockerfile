FROM maprtech/dev-sandbox-container:8.0.0_10.0.0_ubuntu22

LABEL org.opencontainers.image.ref.name=ubuntu
LABEL org.opencontainers.image.version=22.04
LABEL org.opencontainers.image.authors="erdincka@msn.com"

ARG MAPR_REPO=https://package.ezmeral.hpe.com/releases/
ARG UBUNTU_VERSION=22.04
# ARG NIFI_VERSION=1.28.0
# ARG MYSQL_CONNECTOR_VERSION=9.3.0

ENV DEBIAN_FRONTEND=noninteractive

# Replace dev repo
RUN sed -i "s|http://dfaf.mip.storage.hpecorp.net/artifactory/list/prestage/releases-dev|${MAPR_REPO}|g" /etc/apt/sources.list.d/mapr.list
RUN sed -i "s|http://dfaf.mip.storage.hpecorp.net/artifactory/list/prestage/releases|${MAPR_REPO}|g" /etc/apt/sources.list.d/mapr.list

RUN apt update && apt install -y git python3-dev gcc tree locales

ENV MAPR_HOME=/opt/mapr
ENV SHELL=/bin/bash \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8
RUN locale-gen $LC_ALL

RUN apt update && apt upgrade -y
RUN apt install -y \
    mapr-drill \
    mapr-spark \
    mapr-spark-thriftserver \
    mysql-client
# mapr-nifi \

# mapr-hue \
# mapr-airflow \
# mapr-collectd \
# mapr-opentsdb \
# mapr-grafana \
# RUN apt remove -y mapr-nfs && apt install -y mapr-nfs4server

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

# Possible services to expose, WebUI, App, SSH, Drill, Airflow, NiFi, Grafana, Hue ## TODO: add spark-historyserver
EXPOSE 8443 8501 22 8047 8780 3000 8888 
# 12443

# fix init-script
# RUN sed -i '/after cldb /a while [[ ! -f /tmp/maprticket_0 ]]; do (echo mapr | maprlogin password -user mapr); sleep 1; done' /usr/bin/init-script

# Add CDC support to NiFi
# RUN wget -P /opt/mapr/nifi/nifi-"${NIFI_VERSION}"/extensions https://repo1.maven.org/maven2/org/apache/nifi/nifi-cdc-mysql-nar/"${NIFI_VERSION}"/nifi-cdc-mysql-nar-"${NIFI_VERSION}".nar
# Enable Iceberg for Spark
# RUN wget -P /opt/mapr/spark/spark-3.5.5/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

# RUN wget "https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j_${MYSQL_CONNECTOR_VERSION}-1ubuntu${UBUNTU_VERSION}_all.deb"
# RUN dpkg -i "./mysql-connector-j_${MYSQL_CONNECTOR_VERSION}-1ubuntu${UBUNTU_VERSION}_all.deb"

# Set up files/scripts
COPY ./mysql.cnf /etc/mysql/conf.d/client.cnf
COPY entrypoint.sh /
COPY create_iceberg_table.py /home/mapr/
COPY invoke-pyspark.sh /home/mapr/
COPY ingest_to_iceberg.py /home/mapr/

# Cleanup
# RUN rm "./mysql-connector-j_${MYSQL_CONNECTOR_VERSION}-1ubuntu${UBUNTU_VERSION}_all.deb"

ENTRYPOINT [ "/entrypoint.sh" ]

HEALTHCHECK --interval=30s --timeout=30s --start-period=15m --start-interval=30s --retries=10 CMD [ -d /mapr/maprdemo.mapr.io/ ] || exit 1
