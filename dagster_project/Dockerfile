FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY requirements.txt /opt/project/requirements.txt
RUN pip install -r /opt/project/requirements.txt

ENV DAGSTER_HOME=/opt/dagster/home
RUN mkdir -p $DAGSTER_HOME

COPY shared /opt/project/shared
COPY sql /opt/project/sql
COPY dagster_project /opt/project/dagster_project

ENV PYTHONPATH=/opt/project
