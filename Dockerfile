# This is an example Dockerfile to run your service in PaaSTA!
# It satisfies the PaaSTA contract.

# See https://confluence.yelpcorp.com/display/ENG/Docker+Best+Practices
# for best practices on how to make changes to this file.
FROM    docker-dev.yelpcorp.com/xenial_yelp:latest

# python and uwsgi deps
RUN     apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
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
            tox \
            virtualenv \
            zk-flock \
        && apt-get clean

# See https://confluence.yelpcorp.com/display/~asottile/GettingPythonOffLucid
# and https://migration-status.dev.yelp.com/metric/ToxNonLucid
# for more information (e.g., using pip-custom-platform, tox virtualenv build, etc)
COPY    tox.ini requirements.txt requirements-bootstrap.txt /code/
RUN     cd code && tox -e virtualenv_run

# User "nobody" needs to check out the clusterman_signals Git repo so it needs SSH
# keys and a place to put the repo; the SSH keys get mounted as an extra_volumes
# parameter in yelpsoa_configs
RUN     mkdir -p /home/nobody/.ssh /home/nobody/.cache/clusterman && chown -R nobody /home/nobody
RUN     usermod -d /home/nobody nobody
RUN     echo 'Host sysgit.yelpcorp.com\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null' > /home/nobody/.ssh/config

# Code is COPY'ed here after the pip install above, so that code changes do not
# break the preceding cache layer.
COPY    . /code

# Use yelp-compose (y/ycp) for acceptance testing
RUN     install -d --owner=nobody /code/logs

# Create /nail/run to store the batch PID file
RUN     mkdir -p /nail/run && chown -R nobody /nail/run

# For sake of security, don't run your service as a privileged user
USER    nobody
WORKDIR /code
ENV     BASEPATH=/code PATH=/code/virtualenv_run/bin:$PATH
