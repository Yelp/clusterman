ARG DOCKER_REGISTRY
ARG IMAGE_NAME
FROM ${DOCKER_REGISTRY}/${IMAGE_NAME}

RUN apt-get update -yq && apt-get upgrade -y && \
    apt-get install -yq \
        # needed to add a ppa
        software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa



RUN     apt-get -yq update && apt-get install -yq --no-install-recommends \
            debhelper \
            dh-virtualenv \
            dpkg-dev \
            gcc \
            gdebi-core \
            libfreetype6 \
            libatlas-base-dev \
            libatlas3-base \
            libxau-dev \
            libpng-dev \
            libbrotli-dev \
            liblzma-dev \
            libjpeg-dev \
            libwebp-dev \
            libwebpmux3 \
            libyaml-dev \
            python3.8-dev \
            python3-pip \
            python3.8-distutils

RUN ldconfig

WORKDIR /work
