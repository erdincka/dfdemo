FROM erdincka/dfclient:latest

LABEL org.opencontainers.image.ref.name=ubuntu
LABEL org.opencontainers.image.version=20.04
LABEL org.opencontainers.image.authors="erdincka@msn.com"

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/opt/mapr/lib
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN apt update && apt install -y locales netbase sshpass git python3-pip
ENV SHELL=/bin/bash \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8
RUN locale-gen $LC_ALL

# # Enable streams and db clients
RUN pip3 install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" mapr-streams-python
RUN pip3 install maprdb-python-client

# Ready mount point
RUN [ -d /mapr ] || mkdir /mapr

# App
RUN git clone --depth=1 https://github.com/erdincka/catchx.git /app

RUN pip install nicegui==1.4.37 requests importlib_resources \
    faker pyiceberg[hive,pandas,s3fs] deltalake sqlalchemy \
    country_converter pycountry PyMySQL minio
# override versions installed by others
RUN pip install protobuf==3.20.*

WORKDIR /app
ENTRYPOINT [ "python3", "main.py" ]
