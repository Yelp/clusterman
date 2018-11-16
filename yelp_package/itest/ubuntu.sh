#!/bin/bash

set -euxo pipefail

cd /

highlight() {
  echo -n "$(tput setaf 3)"
  echo -n "$@"
  echo "$(tput op)"
}

highlight_exec() {
  highlight "$@"
  command "$@"
  return $?
}

PACKAGE_NAME="$1"
PACKAGE_VERSION="$2"

# This will get DISTRIB_CODENAME
source /etc/lsb-release

# Set up the timezone so clusterman_metrics gets the right data
export TZ=US/Pacific
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# This will set us up to install our package through apt-get
highlight "Creating new apt source"
echo "deb file:/dist/${DISTRIB_CODENAME} ./" | tee "/etc/apt/sources.list.d/itest-${PACKAGE_NAME}.list"

apt-get update
apt-get install -y --force-yes git make tox python3-pip python3-yaml aws-cli
apt-get install -y --force-yes  -f "${PACKAGE_NAME}=${PACKAGE_VERSION}"

AWS_ENDPOINT_URL_ARGS='--endpoint-url http://moto-s3:5000'

aws ${AWS_ENDPOINT_URL_ARGS} s3 mb "s3://yelp-clusterman-signals/"
aws ${AWS_ENDPOINT_URL_ARGS} s3 cp /itest/clusterman_signals_acceptance.tar.gz "s3://yelp-clusterman-signals/${DISTRIB_CODENAME}/clusterman_signals_acceptance.tar.gz"

pip3 install boto3 simplejson
python3 /itest/run_instance.py

rm -rf /opt/venvs/clusterman/lib/python3.6/site-packages/clog  # pretend we're in a non-Yelp env

# Run the critical clusterman CLI commands
highlight_exec /usr/bin/clusterman --version
highlight_exec /usr/bin/clusterman status --cluster docker -v
highlight_exec /usr/bin/clusterman manage --cluster docker --target-capacity 10 --dry-run
highlight_exec /usr/bin/clusterman simulate --cluster docker --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz
highlight_exec /usr/bin/clusterman --log-level debug simulate --cluster docker --autoscaler-config /itest/autoscaler_config.yaml --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz

highlight "$0:" 'success!'
