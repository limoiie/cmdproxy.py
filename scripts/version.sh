#!/usr/bin/env bash

VERSION=`python -c 'import cmdproxy; print(cmdproxy.__version__.replace("+", "-"))'`
export CMDPROXY_VERSION=$VERSION
echo $CMDPROXY_VERSION
