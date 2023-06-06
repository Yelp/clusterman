ARG DOCKER_REGISTRY
ARG IMAGE_NAME
FROM ${DOCKER_REGISTRY}/${IMAGE_NAME}
# TODO: remove this once we're no longer using py3.7 for clusterman
RUN apt-get update -yq && \
    # TODO: this really shouldn't install recommended packages, but luisp gave up on trying to figure
    # out what else was needed to get adding a ppa to work on jammy. in any case, this Dockerfile is
    # only used for building debs so having a little bloat is fine
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
            # needed for numpy
            libatlas3-base \
            libyaml-dev \
            python3.8 \
            python3-pip \
            python3.8-distutils
WORKDIR /work
