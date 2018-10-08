#!/bin/bash
set -ex

VERSIONS=(${CMAN_SIGNAL_VERSIONS})
NAMESPACES=(${CMAN_SIGNAL_NAMESPACES})
NAMES=(${CMAN_SIGNAL_NAMES})
APPS=(${CMAN_SIGNAL_APPS})
version="clusterman_signals_${VERSIONS[$1]}"
namespace="${NAMESPACES[$1]}"
name="${NAMES[$1]}"
app="${APPS[$1]}"

cd ${version}
prodenv/bin/python -m clusterman_signals.run ${namespace} ${name} ${app}
