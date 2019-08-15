# This is an example Dockerfile to run your service in PaaSTA!
# It satisfies the PaaSTA contract.
FROM    ubuntu:bionic

# python and uwsgi deps
RUN     apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            awscli \
            g++ \
            git \
            libatlas-base-dev \
            libxml2 \
            libyaml-0-2 \
            make \
            openssh-client \
            python3.7-dev \
            python3-pip \
            python-setuptools \
            virtualenv \
        && apt-get clean

RUN     /usr/bin/pip3 install supervisor tox
COPY    tox.ini requirements.txt requirements-bootstrap.txt /code/
RUN     cd code && tox -e virtualenv_run

RUN     mkdir /home/nobody
ENV     HOME /home/nobody

# Code is COPY'ed here after the pip install above, so that code changes do not
# break the preceding cache layer.
COPY    . /code
RUN     chown nobody /code

# This is needed so that we can pass PaaSTA itests on Jenkins; for some reason (probably aufs-related?)
# root can't modify the contents of /code on Jenkins, even though it works locally.  Root needs to
# modify these contents so that it can configure the Dockerized Mesos cluster that we run our itests on.
# This shouldn't be a security risk because we drop privileges below and on overlay2, root can already
# modify the contents of this directory.
RUN     chmod -R 775 /code
RUN     ln -s /code/clusterman/supervisord/fetch_clusterman_signal /usr/bin/fetch_clusterman_signal
RUN     ln -s /code/clusterman/supervisord/run_clusterman_signal /usr/bin/run_clusterman_signal

RUN     install -d --owner=nobody /code/logs

# Create /nail/run to store the batch PID file
RUN     mkdir -p /nail/run && chown -R nobody /nail/run

# For sake of security, don't run your service as a privileged user
USER    nobody
WORKDIR /code
ENV     BASEPATH=/code PATH=/code/virtualenv_run/bin:$PATH DISTRIB_CODENAME=bionic
