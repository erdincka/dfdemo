FROM erdincka/dfclient:latest

LABEL org.opencontainers.image.ref.name=ubuntu
LABEL org.opencontainers.image.version=20.04
LABEL org.opencontainers.image.authors="erdincka@msn.com"

EXPOSE 8501

COPY pyproject.toml uv.lock .python-version /app/
COPY . /app/

WORKDIR /app
COPY entrypoint.sh /
COPY .streamlit /app/
RUN /root/.local/bin/uv sync

ENV PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/mapr/bin

RUN echo mapr > /root/mapr_password

RUN apt -qq install -y ca-certificates mysql-client sudo vim 
COPY create-demo-table.sql /
COPY ./mysql.cnf /etc/mysql/conf.d/client.cnf

ENTRYPOINT [ "/entrypoint.sh" ]

HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 CMD curl -f http://localhost:8501/ || exit 1
