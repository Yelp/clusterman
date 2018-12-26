# This is an example Dockerfile to run your service in PaaSTA!
# It satisfies the PaaSTA contract.

# See https://confluence.yelpcorp.com/display/ENG/Docker+Best+Practices
# for best practices on how to make changes to this file.
FROM    docker-dev.yelpcorp.com/xenial_yelp:latest

# python and uwsgi deps
RUN     apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            awscli \
            dumb-init \
            git \
            libatlas-base-dev \
            libmysqlclient20 \
            libpython3.6 \
            libxml2 \
            libyaml-0-2 \
            make \
            openssh-client \
            python3.6 \
            python-pip \
            python-setuptools \
            stdin2scribe \
            tox \
            virtualenv \
            zk-flock \
        && apt-get clean

RUN    /usr/bin/pip install --index-url https://pypi.yelpcorp.com/simple supervisor

# See https://confluence.yelpcorp.com/display/~asottile/GettingPythonOffLucid
# and https://migration-status.dev.yelp.com/metric/ToxNonLucid
# for more information (e.g., using pip-custom-platform, tox virtualenv build, etc)
COPY    tox.ini requirements.txt requirements-bootstrap.txt /code/
RUN     cd code && tox -e virtualenv_run

RUN     mkdir /home/nobody
ENV     HOME /home/nobody

# Code is COPY'ed here after the pip install above, so that code changes do not
# break the preceding cache layer.
COPY    . /code
RUN     chown nobody /code
RUN     ln -s /code/clusterman/supervisord/fetch_clusterman_signal /usr/bin/fetch_clusterman_signal
RUN     ln -s /code/clusterman/supervisord/run_clusterman_signal /usr/bin/run_clusterman_signal

# Use yelp-compose (y/ycp) for acceptance testing
RUN     install -d --owner=nobody /code/logs

# Create /nail/run to store the batch PID file
RUN     mkdir -p /nail/run && chown -R nobody /nail/run

# For sake of security, don't run your service as a privileged user
USER    nobody
WORKDIR /code
ENV     BASEPATH=/code PATH=/code/virtualenv_run/bin:$PATH
