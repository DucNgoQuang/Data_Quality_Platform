FROM apache/superset:latest

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install mysqlclient 
RUN pip install pyhive
RUN pip install thrift \
    && pip install thrift_sasl \
    && pip install sasl
USER superset