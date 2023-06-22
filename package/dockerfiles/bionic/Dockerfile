ARG DOCKER_REGISTRY
ARG IMAGE_NAME
FROM ${DOCKER_REGISTRY}/${IMAGE_NAME}

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
