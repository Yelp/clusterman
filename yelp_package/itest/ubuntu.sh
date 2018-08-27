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
highlight_exec apt-get update
highlight_exec apt-get install -y --force-yes git make tox
highlight_exec dpkg -i "/dist/${DISTRIB_CODENAME}/${PACKAGE_NAME}_${PACKAGE_VERSION}-${SHA}_amd64.deb" || true
highlight_exec apt-get install -y --force-yes  -f

echo -e "Host sysgit.yelpcorp.com\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile=/dev/null" > $HOME/.ssh/config

# Run the critical clusterman CLI commands
highlight_exec /usr/bin/clusterman --version
highlight_exec /usr/bin/clusterman status --cluster everywhere-testopia --pool jolt -v
highlight_exec /usr/bin/clusterman manage --cluster everywhere-testopia --pool jolt --target-capacity 10 --dry-run
highlight_exec /usr/bin/clusterman simulate --cluster everywhere-testopia --pool jolt
highlight_exec /usr/bin/clusterman --log-level debug simulate --cluster everywhere-testopia --pool jolt --autoscaler-config /itest/autoscaler_config.yaml

highlight "$0:" 'success!'
