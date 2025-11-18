#!/bin/bash

set -eu

pushd repo

nix develop -c make check-code
