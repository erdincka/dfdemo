FROM --platform=linux/amd64 maprtech/dev-sandbox-container:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y git python3-dev gcc tree locales
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8

# fix init-script
RUN sed -i '/after cldb /a     sleep 30; echo mapr | maprlogin password -user mapr' /usr/bin/init-script

EXPOSE 9443 8443 8501 8502 2222

COPY . /app
WORKDIR /app

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN echo "export UV_ENV_FILE=.env" >> $HOME/.profile
RUN echo "export LD_LIBRARY_PATH=/opt/mapr/lib" >> $HOME/.profile
ENV CFLAGS=-I/opt/mapr/include
ENV LDFLAGS=-L/opt/mapr/lib

RUN . $HOME/.local/bin/env && uv add mapr-streams-python
