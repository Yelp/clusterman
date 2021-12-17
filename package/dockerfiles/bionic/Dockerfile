ARG DOCKER_REGISTRY=docker-dev.yelpcorp.com/
FROM ${DOCKER_REGISTRY}ubuntu:bionic

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

ARG CLUSTERMAN_ENV=YELP
ENV CLUSTERMAN_ENV=$CLUSTERMAN_ENV

RUN     apt-get -yq update && apt-get install -yq --no-install-recommends \
            debhelper \
            dh-virtualenv \
            dpkg-dev \
            gcc \
            gdebi-core \
            libfreetype6 \
            libatlas-base-dev \
            libyaml-dev \
            python3.7-dev \
            python-pip

WORKDIR /work
