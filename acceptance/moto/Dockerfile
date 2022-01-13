ARG DOCKER_REGISTRY
ARG IMAGE_NAME

FROM ${DOCKER_REGISTRY}/${IMAGE_NAME}

RUN apt-get -yq update && apt-get install -yq --no-install-recommends \
    gcc \
    python3-dev \
    libffi-dev \
    python3 \
    libssl-dev \
    python3-pip

ADD . /moto/
ENV PYTHONUNBUFFERED 1

WORKDIR /moto/
# Setuptools needs to be installed and up-to-date for install of the actual packages
#
# moto and botocore have mismatched upper-bound pins for python-dateutils
# which breaks our build.  botocore used to have <3.0.0, but shrunk that to
# <2.8.1, and moto hasn't updated their pin to match yet.  So until those
# are fixed, here's the latest version of boto that has the <3.0.0 pin.
#
# We can unpin boto3 and botocore once botocore fixes its pin
# (see https://github.com/boto/botocore/commit/e87e7a745fd972815b235a9ee685232745aa94f9)
RUN pip3 install pip==21.3.1 setuptools==59.6.0 && \
    pip3 install cryptography==3.2 botocore==1.14.11 boto3==1.11.11 "moto[server]"

ENTRYPOINT ["python3", "-m", "moto.server", "-H", "0.0.0.0"]

EXPOSE 5000
