#!/bin/bash

#! Auto synced from Shared CI Resources repository
#! Don't change this file, instead change it in github.com/GaloyMoney/concourse-shared

echo "    --> git config"
if [[ -z $(git config --global user.email) ]]; then
  git config --global user.email "202112752+blinkbitcoinbot@users.noreply.github.com"
fi
if [[ -z $(git config --global user.name) ]]; then
  git config --global user.name "CI blinkbitcoinbot"
fi

export CARGO_HOME="$(pwd)/cargo-home"
export CARGO_TARGET_DIR="$(pwd)/cargo-target-dir"

unpack_deps() { echo ""; }
