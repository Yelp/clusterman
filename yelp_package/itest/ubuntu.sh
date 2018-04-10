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

# Install the package from our pre-built deb; this will fail because of dependencies, but ignore
# the error and then run apt-get to fix up the dependencies
highlight_exec apt-get update
highlight_exec dpkg -i "/dist/${DISTRIB_CODENAME}/${PACKAGE_NAME}_${PACKAGE_VERSION}-${SHA}_amd64.deb" || true
highlight_exec apt-get install -y --force-yes  -f

# Run the critical clusterman CLI commands
highlight_exec /usr/bin/clusterman --version
highlight_exec /usr/bin/clusterman status --cluster everywhere-testopia --role jolt -v
highlight_exec /usr/bin/clusterman manage --cluster everywhere-testopia --role jolt --target-capacity 10 --dry-run
highlight_exec /usr/bin/clusterman simulate --cluster everywhere-testopia --role jolt

highlight "$0:" 'success!'
