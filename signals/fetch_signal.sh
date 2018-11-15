#!/bin/bash
set -ex

. /etc/boto_cfg/clusterman.sh

SIGNAL_DIR=${2:-.}
PLATFORM=$(lsb_release -c | cut -f2)
VERSIONS=(${CMAN_VERSIONS_TO_FETCH})
version="clusterman_signals_${VERSIONS[$1]}"
mkdir -p ${SIGNAL_DIR}/${version}
cd ${SIGNAL_DIR}/${version}
aws s3 cp "s3://${CMAN_SIGNALS_BUCKET}/${PLATFORM}/${version}.tar.gz" .
tar -xzf "${version}.tar.gz"
