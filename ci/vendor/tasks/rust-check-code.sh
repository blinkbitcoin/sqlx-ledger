#!/bin/bash

#! Auto synced from Shared CI Resources repository
#! Don't change this file, instead change it in github.com/blinkbitcoin/concourse-shared

set -eu

pushd repo

if command -v nix &> /dev/null; then
  nix develop -c make check-code
else
  make check-code
fi
