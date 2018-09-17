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
SHA="$3"

# This will get DISTRIB_CODENAME
source /etc/lsb-release

# Set up the timezone so clusterman_metrics gets the right data
export TZ=US/Pacific
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install the package from our pre-built deb; this will fail because of dependencies, but ignore
# the error and then run apt-get to fix up the dependencies
apt-get update
apt-get install -y --force-yes git make tox python3-pip python3-yaml
dpkg -i "/dist/${DISTRIB_CODENAME}/${PACKAGE_NAME}_${PACKAGE_VERSION}-${SHA}_amd64.deb" || true
apt-get install -y --force-yes  -f

pip3 install boto3 simplejson
python3 /itest/run_instance.py

rm -rf /opt/venvs/clusterman/lib/python3.6/site-packages/clog  # pretend we're in a non-Yelp env

echo -e "Host sysgit.yelpcorp.com\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile=/dev/null" > $HOME/.ssh/config

# Run the critical clusterman CLI commands
highlight_exec /usr/bin/clusterman --version
highlight_exec /usr/bin/clusterman status --cluster docker -v
highlight_exec /usr/bin/clusterman manage --cluster docker --target-capacity 10 --dry-run
highlight_exec /usr/bin/clusterman simulate --cluster docker --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz
highlight_exec /usr/bin/clusterman --log-level debug simulate --cluster docker --autoscaler-config /itest/autoscaler_config.yaml --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz

highlight "$0:" 'success!'
