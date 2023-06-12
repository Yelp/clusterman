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
            libatlas3-base \
            libwebp-dev \
            libwebpmux3 \
            libyaml-dev \
            python3.8 \
            python3-pip \
            python3.8-distutils
WORKDIR /work
