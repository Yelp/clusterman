FROM    docker-dev.yelpcorp.com/trusty_pkgbuild

# TODO (COREBACK-6466) Remove the force-overwrite option once we fix things with curl
RUN     apt-get -yq update && apt-get -yq dist-upgrade -o Dpkg::Options::="--force-overwrite" && \
            apt-get install -yq --no-install-recommends libfreetype6 libatlas-base-dev
RUN     mkdir -p /etc/boto_cfg
WORKDIR /work
