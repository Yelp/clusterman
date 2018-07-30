FROM    docker-dev.yelpcorp.com/trusty_pkgbuild
RUN     apt-get -yq update && apt-get -yq dist-upgrade && \
            apt-get install -yq --no-install-recommends libfreetype6 libatlas-base-dev
RUN     mkdir -p /etc/boto_cfg
WORKDIR /work
