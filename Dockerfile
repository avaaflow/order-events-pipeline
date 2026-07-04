FROM repo-nexus.kavosh.org:8080/python:3.11-slim

ARG PIP_PROXY=http://proxy-us-p3.kavosh.org:26
ARG PIP_INDEX_URL=https://pypi.org/simple

ENV PYTHONUNBUFFERED=1 \
    HTTP_PROXY=${PIP_PROXY} \
    HTTPS_PROXY=${PIP_PROXY} \
    http_proxy=${PIP_PROXY} \
    https_proxy=${PIP_PROXY} \
    PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update -o Acquire::http::Proxy="${PIP_PROXY}" \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY settings.py .
COPY generators/ generators/
COPY producers/ producers/
COPY consumers/ consumers/
