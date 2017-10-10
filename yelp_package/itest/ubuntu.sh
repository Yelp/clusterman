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

# This will set us up to install our package through apt-get
highlight "Creating new apt source"
echo "deb file:/dist/${DISTRIB_CODENAME} ./" | tee "/etc/apt/sources.list.d/itest-${PACKAGE_NAME}.list"

highlight_exec apt-get update

# The package should install ok
highlight_exec apt-get install -y --force-yes "${PACKAGE_NAME}=${PACKAGE_VERSION}"

# TODO: implement your integration tests for Ubuntu here.
highlight_exec /usr/bin/clusterman --version

highlight "$0:" 'success!'
